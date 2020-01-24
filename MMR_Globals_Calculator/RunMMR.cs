using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using MMR_Globals_Calculator.Database.HeroesProfile;
using MMR_Globals_Calculator.Models;
using MySql.Data.MySqlClient;
using Z.EntityFramework.Plus;

namespace MMR_Globals_Calculator
{
    public class RunMmrResult
    {
        public Dictionary<string, string> Roles = new Dictionary<string, string>();
        public Dictionary<string, string> Heroes = new Dictionary<string, string>();
        public Dictionary<string, string> HeroesIds = new Dictionary<string, string>();
        public Dictionary<int, int> ReplaysToRun = new Dictionary<int, int>();
        public Dictionary<string, string> Players = new Dictionary<string, string>();
        public Dictionary<string, string> SeasonsGameVersions = new Dictionary<string, string>();
    }
    internal class RunMmrService
    {
        private readonly DbSettings _dbSettings;
        private readonly string _connectionString;
        private readonly HeroesProfileContext _context;
        private readonly MmrCalculatorService _mmrCalculatorService;

        public RunMmrService(DbSettings dbSettings, HeroesProfileContext context, MmrCalculatorService mmrCalculatorService)
        {
            _context = context;
            _dbSettings = dbSettings;
            _mmrCalculatorService = mmrCalculatorService;
            _connectionString = ConnectionStringBuilder.BuildConnectionString(_dbSettings);
        }


        public RunMmrResult RunMmr()
        {
            var result = new RunMmrResult();

            var mmrTypeIds = _context.MmrTypeIds.ToList();

            var heroes = _context.Heroes.Select(x => new {x.Id, x.Name, x.NewRole}).ToList();

            foreach (var hero in heroes)
            {
                result.Heroes.Add(hero.Name, hero.Id.ToString());
                result.HeroesIds.Add(hero.Id.ToString(), hero.Name);
                result.Roles.Add(hero.Name, hero.NewRole);
            }

            var seasonGameVersions = _context.SeasonGameVersions.ToList();

            foreach (var version in seasonGameVersions.Where(version => !result.SeasonsGameVersions.ContainsKey(version.GameVersion)))
            {
                result.SeasonsGameVersions.Add(version.GameVersion, version.Season.ToString());
            }

            var replays = _context.Replay
                .Where(x => x.MmrRan == 0)
                .OrderBy(x => x.GameDate)
                .Join(
                _context.Player,
                replay => replay.ReplayId,
                player => player.ReplayId,
                (replay, player) => new
                {
                    replay.ReplayId,
                    replay.Region,
                    player.BlizzId
                })
                .Take(10000)
                .ToList();

            foreach (var replay in replays.Where(replay => !result.Players.ContainsKey(replay.BlizzId + "|" + replay.Region)))
            {
                if (!result.ReplaysToRun.ContainsKey((int) replay.ReplayId))
                {
                    //TODO: Are these supposed to both be ReplayId?
                    result.ReplaysToRun.Add((int) replay.ReplayId, (int) replay.ReplayId);
                }
                result.Players.Add(replay.BlizzId + "|" + replay.Region, replay.BlizzId + "|" + replay.Region);
            }

            Console.WriteLine("Finished  - Sleeping for 5 seconds before running");
            System.Threading.Thread.Sleep(5000);

            Parallel.ForEach(
                result.ReplaysToRun.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                new ParallelOptions {MaxDegreeOfParallelism = 1},
                //new ParallelOptions { MaxDegreeOfParallelism = 10 },
                replayId =>
                {
                    Console.WriteLine("Running MMR data for replayID: " + replayId);
                    var data = GetReplayData(replayId, result.Roles, result.HeroesIds);
                    if (data.Replay_Player == null) return;
                    if (data.Replay_Player.Length != 10 || data.Replay_Player[9] == null) return;
                    data = CalculateMmr(data, mmrTypeIds, result.Roles);
                    UpdatePlayerMmr(data);
                    SaveMasterMmrData(data, mmrTypeIds.ToDictionary(x => x.Name, x => x.MmrTypeId), result.Roles);

                    _context.Replay
                        .Where(x => x.ReplayId == replayId)
                        .Update(x => new Replay {MmrRan = 1});

                    if (Convert.ToInt32(result.SeasonsGameVersions[data.GameVersion]) < 13) return;
                    {
                        using var conn = new MySqlConnection(_connectionString);
                        conn.Open();
                        UpdateGlobalHeroData(data, conn);
                        UpdateGlobalTalentData(data, conn);
                        UpdateGlobalTalentDataDetails(data, conn);
                        UpdateMatchups(data);
                        UpdateDeathwingData(data, conn);
                    }
                });
            return result;
        }

        private ReplayData GetReplayData(int replayId, Dictionary<string, string> roles, Dictionary<string, string> heroIds)
        {
            var data = new ReplayData();
            var players = new ReplayPlayer[10];
            var playerCounter = 0;

            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT " +
                                  "replay.replayID, " +
                                  "replay.game_type, " +
                                  "replay.region, " +
                                  "replay.game_date, " +
                                  "replay.game_length, " +
                                  "replay.game_version, " +
                                  "replay.game_map, " +
                                  "player.hero, " +
                                  "player.blizz_id, " +
                                  "player.winner, " +
                                  "player.hero_level, " +
                                  "player.mastery_taunt, " +
                                  "player.team, " +
                                  "if(talents.level_one is null, 0, talents.level_one) as level_one, " +
                                  "if(talents.level_four is null, 0, talents.level_four) as level_four, " +
                                  "if(talents.level_seven is null, 0, talents.level_seven) as level_seven, " +
                                  "if(talents.level_ten is null, 0, talents.level_ten) as level_ten, " +
                                  "if(talents.level_thirteen is null, 0, talents.level_thirteen) as level_thirteen, " +
                                  "if(talents.level_sixteen is null, 0, talents.level_sixteen) as level_sixteen, " +
                                  "if(talents.level_twenty is null, 0, talents.level_twenty) as level_twenty, " +
                                  "if(kills is null, 0, kills) as kills, " +
                                  "if(assists is null, 0, assists) as assists, " +
                                  "if(takedowns is null, 0, takedowns) as takedowns, " +
                                  "if(deaths is null, 0, deaths) as deaths, " +
                                  "if(highest_kill_streak is null, 0, highest_kill_streak) as highest_kill_streak, " +
                                  "if(hero_damage is null, 0, hero_damage) as hero_damage, " +
                                  "if(siege_damage is null, 0, siege_damage) as siege_damage, " +
                                  "if(structure_damage is null, 0, structure_damage) as structure_damage, " +
                                  "if(minion_damage is null, 0, minion_damage) as minion_damage, " +
                                  "if(creep_damage is null, 0, creep_damage) as creep_damage, " +
                                  "if(summon_damage is null, 0, summon_damage) as summon_damage, " +
                                  "if(time_cc_enemy_heroes is null, 0, time_cc_enemy_heroes) as time_cc_enemy_heroes, " +
                                  "if(healing is null, 0, healing) as healing, " +
                                  "if(self_healing is null, 0, self_healing) as self_healing, " +
                                  "if(damage_taken is null, 0, damage_taken) as damage_taken, " +
                                  "if(experience_contribution is null, 0, experience_contribution) as experience_contribution, " +
                                  "if(town_kills is null, 0, town_kills) as town_kills, " +
                                  "if(time_spent_dead is null, 0, time_spent_dead) as time_spent_dead, " +
                                  "if(merc_camp_captures is null, 0, merc_camp_captures) as merc_camp_captures, " +
                                  "if(watch_tower_captures is null, 0, watch_tower_captures) as watch_tower_captures, " +
                                  "if(meta_experience is null, 0, meta_experience) as meta_experience, " +
                                  "if(match_award is null, 0, match_award) as match_award, " +
                                  "if(protection_allies is null, 0, protection_allies) as protection_allies, " +
                                  "if(silencing_enemies is null, 0, silencing_enemies) as silencing_enemies, " +
                                  "if(rooting_enemies is null, 0, rooting_enemies) as rooting_enemies, " +
                                  "if(stunning_enemies is null, 0, stunning_enemies) as stunning_enemies, " +
                                  "if(clutch_heals is null, 0, clutch_heals) as clutch_heals, " +
                                  "if(escapes is null, 0, escapes) as escapes, " +
                                  "if(vengeance is null, 0, vengeance) as vengeance, " +
                                  "if(outnumbered_deaths is null, 0, outnumbered_deaths) as outnumbered_deaths, " +
                                  "if(teamfight_escapes is null, 0, teamfight_escapes) as teamfight_escapes, " +
                                  "if(teamfight_healing is null, 0, teamfight_healing) as teamfight_healing, " +
                                  "if(teamfight_damage_taken is null, 0, teamfight_damage_taken) as teamfight_damage_taken, " +
                                  "if(teamfight_hero_damage is null, 0, teamfight_hero_damage) as teamfight_hero_damage, " +
                                  "if(multikill is null, 0, multikill) as multikill, " +
                                  "if(physical_damage is null, 0, physical_damage) as physical_damage, " +
                                  "if(spell_damage is null, 0, spell_damage) as spell_damage, " +
                                  "if(regen_globes is null, 0, regen_globes) as regen_globes, " +
                                  "if(first_to_ten is null, 0, first_to_ten) as first_to_ten " +
                                  "FROM " +
                                  "replay " +
                                  "INNER JOIN " +
                                  "player ON player.replayID = replay.replayID " +
                                  "INNER JOIN " +
                                  "talents ON(talents.replayID = replay.replayID " +
                                  "AND talents.battletag = player.battletag) " +
                                  "INNER JOIN " +
                                  "scores ON(scores.replayID = replay.replayID " +
                                  "AND scores.battletag = player.battletag) WHERE replay.replayID = " + replayId + " order by team ASC";

                //Console.WriteLine(cmd.CommandText);
                var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    data.Id = reader.GetInt64("replayID");
                    data.GameType_id = reader.GetString("game_type");
                    data.Region = reader.GetInt64("region");
                    data.GameDate = reader.GetDateTime("game_date");
                    data.Length = reader.GetInt64("game_length");

                    data.GameVersion = reader.GetString("game_version");

                    data.Bans = GetBans(data.Id);

                    data.GameMap_id = reader.GetString("game_map");

                    var player = new ReplayPlayer {Hero_id = reader.GetString("hero"), Hero = heroIds[reader.GetString("hero")]};
                    player.Role = roles[player.Hero];
                    player.BlizzId = reader.GetInt64("blizz_id");

                    var winner = reader.GetInt32("winner");

                    player.Winner = winner == 1;
                    player.HeroLevel = reader.GetInt64("hero_level");
                    player.MasteryTaunt = reader.GetInt64("mastery_taunt");
                    player.Mirror = 0;
                    player.Team = reader.GetInt32("team");

                    var talents = new Talents
                    {
                            Level_One = reader.GetInt32("level_one"),
                            Level_Four = reader.GetInt32("level_four"),
                            Level_Seven = reader.GetInt32("level_seven"),
                            Level_Ten = reader.GetInt32("level_ten"),
                            Level_Thirteen = reader.GetInt32("level_thirteen"),
                            Level_Sixteen = reader.GetInt32("level_Sixteen"),
                            Level_Twenty = reader.GetInt32("level_Twenty")
                    };

                    player.Talents = talents;

                    var score = new Score
                    {
                            SoloKills = reader.GetInt64("kills"),
                            Assists = reader.GetInt64("assists"),
                            Takedowns = reader.GetInt64("takedowns"),
                            Deaths = reader.GetInt64("deaths"),
                            HighestKillStreak = reader.GetInt64("highest_kill_streak"),
                            HeroDamage = reader.GetInt64("hero_damage"),
                            SiegeDamage = reader.GetInt64("siege_damage"),
                            StructureDamage = reader.GetInt64("structure_damage"),
                            MinionDamage = reader.GetInt64("minion_damage"),
                            CreepDamage = reader.GetInt64("creep_damage"),
                            SummonDamage = reader.GetInt64("summon_damage"),
                            TimeCCdEnemyHeroes = reader.GetInt64("time_cc_enemy_heroes"),
                            Healing = reader.GetInt64("healing"),
                            SelfHealing = reader.GetInt64("self_healing"),
                            DamageTaken = reader.GetInt64("damage_taken"),
                            ExperienceContribution = reader.GetInt64("experience_contribution"),
                            TownKills = reader.GetInt64("town_kills"),
                            TimeSpentDead = reader.GetInt64("time_spent_dead"),
                            MercCampCaptures = reader.GetInt64("merc_camp_captures"),
                            WatchTowerCaptures = reader.GetInt64("watch_tower_captures"),
                            ProtectionGivenToAllies = reader.GetInt64("protection_allies"),
                            TimeSilencingEnemyHeroes = reader.GetInt64("silencing_enemies"),
                            TimeRootingEnemyHeroes = reader.GetInt64("rooting_enemies"),
                            TimeStunningEnemyHeroes = reader.GetInt64("stunning_enemies"),
                            ClutchHealsPerformed = reader.GetInt64("clutch_heals"),
                            EscapesPerformed = reader.GetInt64("escapes"),
                            VengeancesPerformed = reader.GetInt64("vengeance"),
                            OutnumberedDeaths = reader.GetInt64("outnumbered_deaths"),
                            TeamfightEscapesPerformed = reader.GetInt64("teamfight_escapes"),
                            TeamfightHealingDone = reader.GetInt64("teamfight_healing"),
                            TeamfightDamageTaken = reader.GetInt64("teamfight_damage_taken"),
                            TeamfightHeroDamage = reader.GetInt64("teamfight_hero_damage"),
                            Multikill = reader.GetInt64("multikill"),
                            PhysicalDamage = reader.GetInt64("physical_damage"),
                            SpellDamage = reader.GetInt64("spell_damage"),
                            RegenGlobes = reader.GetInt64("regen_globes")
                    };
                        
                    player.Score = score;

                    players[playerCounter] = player;
                    playerCounter++;

                }
            }


            if (players.Length == 10 && playerCounter == 10)
            {
                if (players[9] == null) return data;
                data.Replay_Player = players;

                var orderedPlayers = new ReplayPlayer[10];

                var team1 = 0;
                var team2 = 5;
                foreach (var replayPlayer in data.Replay_Player)
                {
                    switch (replayPlayer.Team)
                    {
                        case 0:
                            orderedPlayers[team1] = replayPlayer;
                            team1++;
                            break;
                        case 1:
                            orderedPlayers[team2] = replayPlayer;
                            team2++;
                            break;
                    }
                }
                data.Replay_Player = orderedPlayers;

                for (var i = 0; i < data.Replay_Player.Length; i++)
                {
                    for (var j = 0; j < data.Replay_Player.Length; j++)
                    {
                        if (j == i) continue;
                        if (data.Replay_Player[i].Hero != data.Replay_Player[j].Hero) continue;
                        data.Replay_Player[i].Mirror = 1;
                        break;
                    }
                }

            }
            else
            {
                using var conn = new MySqlConnection(_connectionString);
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "UPDATE replay SET mmr_ran = 1 WHERE replayID = " + replayId;
                var reader = cmd.ExecuteReader();
            }
            return data;
        }
        private int[][] GetBans(long replayId)
        {
            var bans = new int[2][];
            bans[0] = new int[3];
            bans[1] = new int[3];

            bans[0][0] = 0;
            bans[0][1] = 0;
            bans[0][2] = 0;

            bans[1][0] = 0;
            bans[1][1] = 0;
            bans[1][2] = 0;

            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT * FROM heroesprofile.replay_bans where replayID = " + replayId;
                var reader = cmd.ExecuteReader();

                var teamOneCounter = 0;
                var teamTwoCounter = 0;
                while (reader.Read())
                {
                    if (reader.GetInt32("team") == 0)
                    {
                        bans[0][teamOneCounter] = reader.GetInt32("hero");
                        teamOneCounter++;
                    }
                    else
                    {
                        bans[1][teamTwoCounter] = reader.GetInt32("hero");
                        teamTwoCounter++;
                    }
                }
            }

            return bans;
        }
        private ReplayData CalculateMmr(ReplayData data, IEnumerable<MmrTypeIds> mmrTypeIds, Dictionary<string, string> roles)
        {
            var mmrTypeIdsDict = mmrTypeIds.ToDictionary(x => x.Name, x => x.MmrTypeId);

            var mmrCalcPlayer = _mmrCalculatorService.TwoPlayerTestNotDrawn(data, "player", mmrTypeIdsDict, roles);
            data = mmrCalcPlayer;
            var mmrCalcHero = _mmrCalculatorService.TwoPlayerTestNotDrawn(data, "hero", mmrTypeIdsDict, roles);

            //TODO: Should this actually be assigning from mmrCalcHero?
            data = mmrCalcPlayer;
            var mmrCalcRole = _mmrCalculatorService.TwoPlayerTestNotDrawn(data, "role", mmrTypeIdsDict, roles);

            //TODO: Should this actually be assigning from mmrCalcRole?
            data = mmrCalcPlayer;

            data = GetLeagueTierData(data, mmrTypeIdsDict);

            return data;
        }

        private ReplayData GetLeagueTierData(ReplayData data, Dictionary<string, uint> mmrTypeIdsDict)
        {
            foreach (var r in data.Replay_Player)
            {
                r.player_league_tier = GetLeague(mmrTypeIdsDict["player"], data.GameType_id, (1800 + (r.player_conservative_rating * 40)));
                r.hero_league_tier = GetLeague(Convert.ToUInt32(r.Hero_id), data.GameType_id, (1800 + (r.hero_conservative_rating * 40)));
                r.role_league_tier = GetLeague(mmrTypeIdsDict[r.Role], data.GameType_id, (1800 + (r.role_conservative_rating * 40)));
            }
            return data;
        }

        private string GetLeague(uint mmrId, string gameTypeId, double mmr)
        {
            using var conn = new MySqlConnection(_connectionString);
            conn.Open();

            var leagueTier = "";
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT league_tier FROM league_breakdowns where type_role_hero = " + mmrId + " and game_type = " + gameTypeId + " and min_mmr <= " + mmr + " order by min_mmr DESC LIMIT 1";
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    leagueTier = reader.GetString("league_tier");

                }

                if (!reader.HasRows)
                {
                    leagueTier = "1";
                }
            }

            return leagueTier;
        }

        private void UpdatePlayerMmr(ReplayData data)
        {
            using var conn = new MySqlConnection(_connectionString);
            conn.Open();
            foreach (var r in data.Replay_Player)
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "UPDATE player SET " +
                                  "player_conservative_rating = " + r.player_conservative_rating + ", " +
                                  "player_mean = " + r.player_mean + ", " +
                                  "player_standard_deviation = " + r.player_standard_deviation + ", " +
                                  "hero_conservative_rating = " + r.hero_conservative_rating + ", " +
                                  "hero_mean = " + r.hero_mean + ", " +
                                  "hero_standard_deviation = " + r.hero_standard_deviation + ", " +
                                  "role_conservative_rating = " + r.role_conservative_rating + ", " +
                                  "role_mean = " + r.role_mean + ", " +
                                  "role_standard_deviation = " + r.role_standard_deviation + ", " +
                                  "mmr_date_parsed = " + "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" +
                                  " WHERE replayID = " + data.Id +
                                  " AND blizz_id = " + r.BlizzId;

                var reader = cmd.ExecuteReader();
            }
        }

        private void SaveMasterMmrData(ReplayData data, Dictionary<string, uint> mmrTypeIdsDict, Dictionary<string, string> roles)
        {
            using var conn = new MySqlConnection(_connectionString);
            conn.Open();

            foreach (var r in data.Replay_Player)
            {
                var win = 0;
                var loss = 0;

                if (r.Winner)
                {
                    win = 1;
                    loss = 0;
                }
                else
                {
                    win = 0;
                    loss = 1;
                }
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_mmr_data (type_value, game_type, blizz_id, region, conservative_rating, mean, standard_deviation, win, loss) VALUES(" +
                                      "\"" + mmrTypeIdsDict["player"] + "\"" + "," +
                                      data.GameType_id + "," +
                                      r.BlizzId + "," +
                                      data.Region + "," +
                                      r.player_conservative_rating + "," +
                                      r.player_mean + "," +
                                      r.player_standard_deviation + "," +
                                      win + "," +
                                      loss + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                       "type_value = VALUES(type_value), " +
                                       "game_type = VALUES(game_type)," +
                                       "blizz_id = VALUES(blizz_id)," +
                                       "region = VALUES(region)," +
                                       "conservative_rating = VALUES(conservative_rating)," +
                                       "mean = VALUES(mean)," +
                                       "standard_deviation = VALUES(standard_deviation)," +
                                       "win = win + VALUES(win)," +
                                       "loss = loss + VALUES(loss)";

                    cmd.CommandTimeout = 0;
                    //Console.WriteLine(cmd.CommandText);
                    var reader = cmd.ExecuteReader();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_mmr_data (type_value, game_type, blizz_id, region, conservative_rating, mean, standard_deviation, win, loss) VALUES(" +
                                      "\"" + mmrTypeIdsDict[roles[r.Hero]] + "\"" + "," +
                                      data.GameType_id + "," +
                                      r.BlizzId + "," +
                                      data.Region + "," +
                                      r.role_conservative_rating + "," +
                                      r.role_mean + "," +
                                      r.role_standard_deviation + "," +
                                      win + "," +
                                      loss + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                       "type_value = VALUES(type_value), " +
                                       "game_type = VALUES(game_type)," +
                                       "blizz_id = VALUES(blizz_id)," +
                                       "region = VALUES(region)," +
                                       "conservative_rating = VALUES(conservative_rating)," +
                                       "mean = VALUES(mean)," +
                                       "standard_deviation = VALUES(standard_deviation)," +
                                       "win = win + VALUES(win)," +
                                       "loss = loss + VALUES(loss)";
                    cmd.CommandTimeout = 0;
                    //Console.WriteLine(cmd.CommandText);
                    var reader = cmd.ExecuteReader();
                }

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO master_mmr_data (type_value, game_type, blizz_id, region, conservative_rating, mean, standard_deviation, win, loss) VALUES(" +
                                      "\"" + r.Hero_id + "\"" + "," +
                                      data.GameType_id + "," +
                                      r.BlizzId + "," +
                                      data.Region + "," +
                                      r.hero_conservative_rating + "," +
                                      r.hero_mean + "," +
                                      r.hero_standard_deviation + "," +
                                      win + "," +
                                      loss + ")";
                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                       "type_value = VALUES(type_value), " +
                                       "game_type = VALUES(game_type)," +
                                       "blizz_id = VALUES(blizz_id)," +
                                       "region = VALUES(region)," +
                                       "conservative_rating = VALUES(conservative_rating)," +
                                       "mean = VALUES(mean)," +
                                       "standard_deviation = VALUES(standard_deviation)," +
                                       "win = win + VALUES(win)," +
                                       "loss = loss + VALUES(loss)";
                    cmd.CommandTimeout = 0;
                    //Console.WriteLine(cmd.CommandText);
                    var reader = cmd.ExecuteReader();
                }
            }
        }

        private void UpdateGlobalHeroData(ReplayData data, MySqlConnection conn)
        {

            foreach (var player in data.Replay_Player)
            {
                var winLoss = player.Winner ? 1 : 0;

                if (player.Score == null) continue;
                var heroLevel = 0;

                if (player.HeroLevel < 5)
                {
                    heroLevel = 1;
                }
                else if (player.HeroLevel >= 5 && player.HeroLevel < 10)
                {
                    heroLevel = 5;
                }
                else if (player.HeroLevel >= 10 && player.HeroLevel < 15)
                {
                    heroLevel = 10;
                }
                else if (player.HeroLevel >= 15 && player.HeroLevel < 20)
                {
                    heroLevel = 15;
                }
                else if (player.HeroLevel >= 20)
                {
                    heroLevel = player.MasteryTaunt switch
                    {
                            0 => 20,
                            1 => 25,
                            2 => 40,
                            3 => 60,
                            4 => 80,
                            5 => 100,
                            _ => heroLevel
                    };
                }

                using var cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO global_hero_stats (" +
                                  "game_version, " +
                                  "game_type, " +
                                  "league_tier, " +
                                  "hero_league_tier, " +
                                  "role_league_tier, " +
                                  "game_map, " +
                                  "hero_level, " +
                                  "hero, " +
                                  "mirror, " +
                                  "region, " +
                                  "win_loss, " +
                                  "game_time, " +
                                  "kills, " +
                                  "assists, " +
                                  "takedowns, " +
                                  "deaths, " +
                                  "highest_kill_streak, " +
                                  "hero_damage, " +
                                  "siege_damage, " +
                                  "structure_damage, " +
                                  "minion_damage, " +
                                  "creep_damage, " +
                                  "summon_damage, " +
                                  "time_cc_enemy_heroes, " +
                                  "healing, " +
                                  "self_healing, " +
                                  "damage_taken, " +
                                  "experience_contribution, " +
                                  "town_kills, " +
                                  "time_spent_dead, " +
                                  "merc_camp_captures, " +
                                  "watch_tower_captures, " +
                                  "protection_allies, " +
                                  "silencing_enemies, " +
                                  "rooting_enemies, " +
                                  "stunning_enemies, " +
                                  "clutch_heals, " +
                                  "escapes, " +
                                  "vengeance, " +
                                  "outnumbered_deaths, " +
                                  "teamfight_escapes, " +
                                  "teamfight_healing, " +
                                  "teamfight_damage_taken, " +
                                  "teamfight_hero_damage, " +
                                  "multikill, " +
                                  "physical_damage, " +
                                  "spell_damage, " +
                                  "regen_globes, " +
                                  "games_played" +
                                  ") VALUES (" +
                                  "\"" + data.GameVersion + "\"" + "," +
                                  "\"" + data.GameType_id + "\"" + "," +
                                  "\"" + player.player_league_tier + "\"" + "," +
                                  "\"" + player.hero_league_tier + "\"" + "," +
                                  "\"" + player.role_league_tier + "\"" + "," +
                                  "\"" + data.GameMap_id + "\"" + "," +
                                  "\"" + heroLevel + "\"" + "," +
                                  "\"" + player.Hero_id + "\"" + "," +
                                  "\"" + player.Mirror + "\"" + "," +
                                  "\"" + data.Region + "\"" + "," +
                                  "\"" + winLoss + "\"" + "," +
                                  "\"" + data.Length + "\"" + "," +
                                  CheckIfEmpty(player.Score.SoloKills) + "," +
                                  CheckIfEmpty(player.Score.Assists) + "," +
                                  CheckIfEmpty(player.Score.Takedowns) + "," +
                                  CheckIfEmpty(player.Score.Deaths) + "," +
                                  CheckIfEmpty(player.Score.HighestKillStreak) + "," +
                                  CheckIfEmpty(player.Score.HeroDamage) + "," +
                                  CheckIfEmpty(player.Score.SiegeDamage) + "," +
                                  CheckIfEmpty(player.Score.StructureDamage) + "," +
                                  CheckIfEmpty(player.Score.MinionDamage) + "," +
                                  CheckIfEmpty(player.Score.CreepDamage) + "," +
                                  CheckIfEmpty(player.Score.SummonDamage) + "," +
                                  CheckIfEmpty(Convert.ToInt64(player.Score.TimeCCdEnemyHeroes)) + "," +
                                  CheckIfEmpty(player.Score.Healing) + "," +
                                  CheckIfEmpty(player.Score.SelfHealing) + "," +
                                  CheckIfEmpty(player.Score.DamageTaken) + "," +
                                  CheckIfEmpty(player.Score.ExperienceContribution) + "," +
                                  CheckIfEmpty(player.Score.TownKills) + "," +
                                  CheckIfEmpty(Convert.ToInt64(player.Score.TimeSpentDead)) + "," +
                                  CheckIfEmpty(player.Score.MercCampCaptures) + "," +
                                  CheckIfEmpty(player.Score.WatchTowerCaptures) + "," +
                                  CheckIfEmpty(player.Score.ProtectionGivenToAllies) + "," +
                                  CheckIfEmpty(player.Score.TimeSilencingEnemyHeroes) + "," +
                                  CheckIfEmpty(player.Score.TimeRootingEnemyHeroes) + "," +
                                  CheckIfEmpty(player.Score.TimeStunningEnemyHeroes) + "," +
                                  CheckIfEmpty(player.Score.ClutchHealsPerformed) + "," +
                                  CheckIfEmpty(player.Score.EscapesPerformed) + "," +
                                  CheckIfEmpty(player.Score.VengeancesPerformed) + "," +
                                  CheckIfEmpty(player.Score.OutnumberedDeaths) + "," +
                                  CheckIfEmpty(player.Score.TeamfightEscapesPerformed) + "," +
                                  CheckIfEmpty(player.Score.TeamfightHealingDone) + "," +
                                  CheckIfEmpty(player.Score.TeamfightDamageTaken) + "," +
                                  CheckIfEmpty(player.Score.TeamfightHeroDamage) + "," +
                                  CheckIfEmpty(player.Score.Multikill) + "," +
                                  CheckIfEmpty(player.Score.PhysicalDamage) + "," +
                                  CheckIfEmpty(player.Score.SpellDamage) + "," +
                                  CheckIfEmpty(player.Score.RegenGlobes) + "," +
                                  1 + ")";


                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                   "game_time = game_time + VALUES(game_time), " +
                                   "kills = kills + VALUES(kills), " +
                                   "assists = assists + VALUES(assists), " +
                                   "takedowns = takedowns + VALUES(takedowns), " +
                                   "deaths = deaths + VALUES(deaths), " +
                                   "highest_kill_streak = highest_kill_streak + VALUES(highest_kill_streak), " +
                                   "hero_damage = hero_damage + VALUES(hero_damage), " +
                                   "siege_damage = siege_damage + VALUES(siege_damage), " +
                                   "structure_damage = structure_damage + VALUES(structure_damage), " +
                                   "minion_damage = minion_damage + VALUES(minion_damage), " +
                                   "creep_damage = creep_damage + VALUES(creep_damage), " +
                                   "summon_damage = summon_damage + VALUES(summon_damage), " +
                                   "time_cc_enemy_heroes = time_cc_enemy_heroes + VALUES(time_cc_enemy_heroes), " +
                                   "healing = healing + VALUES(healing), " +
                                   "self_healing = self_healing + VALUES(self_healing), " +
                                   "damage_taken = damage_taken + VALUES(damage_taken), " +
                                   "experience_contribution = experience_contribution + VALUES(experience_contribution), " +
                                   "town_kills = town_kills + VALUES(town_kills), " +
                                   "time_spent_dead = time_spent_dead + VALUES(time_spent_dead), " +
                                   "merc_camp_captures = merc_camp_captures + VALUES(merc_camp_captures), " +
                                   "watch_tower_captures = watch_tower_captures + VALUES(watch_tower_captures), " +
                                   "protection_allies = protection_allies + VALUES(protection_allies), " +
                                   "silencing_enemies = silencing_enemies + VALUES(silencing_enemies), " +
                                   "rooting_enemies = rooting_enemies + VALUES(rooting_enemies), " +
                                   "stunning_enemies = stunning_enemies + VALUES(stunning_enemies), " +
                                   "clutch_heals = clutch_heals + VALUES(clutch_heals), " +
                                   "escapes = escapes + VALUES(escapes), " +
                                   "vengeance = vengeance + VALUES(vengeance), " +
                                   "outnumbered_deaths = outnumbered_deaths + VALUES(outnumbered_deaths), " +
                                   "teamfight_escapes = teamfight_escapes + VALUES(teamfight_escapes), " +
                                   "teamfight_healing = teamfight_healing + VALUES(teamfight_healing), " +
                                   "teamfight_damage_taken = teamfight_damage_taken + VALUES(teamfight_damage_taken), " +
                                   "teamfight_hero_damage = teamfight_hero_damage + VALUES(teamfight_hero_damage), " +
                                   "multikill = multikill + VALUES(multikill), " +
                                   "physical_damage = physical_damage + VALUES(physical_damage), " +
                                   "spell_damage = spell_damage + VALUES(spell_damage), " +
                                   "regen_globes = regen_globes + VALUES(regen_globes), " +
                                   "games_played = games_played + VALUES(games_played)";
                //Console.WriteLine(cmd.CommandText);
                var reader = cmd.ExecuteReader();
            }

            double teamOneAvgConservativeRating = 0;
            double teamOneAvgHeroConservativeRating = 0;
            double teamOneAvgRoleConservativeRating = 0;

            double teamTwoAvgConservativeRating = 0;
            double teamTwoAvgHeroConservativeRating = 0;
            double teamTwoAvgRoleConservativeRating = 0;

            foreach (var r in data.Replay_Player)
            {
                if (r.Team == 0)
                {
                    teamOneAvgConservativeRating += Convert.ToDouble(r.player_league_tier);
                    teamOneAvgHeroConservativeRating += Convert.ToDouble(r.hero_league_tier);
                    teamOneAvgRoleConservativeRating += Convert.ToDouble(r.role_league_tier);
                }
                else
                {
                    teamTwoAvgConservativeRating += Convert.ToDouble(r.player_league_tier);
                    teamTwoAvgHeroConservativeRating += Convert.ToDouble(r.hero_league_tier);
                    teamTwoAvgRoleConservativeRating += Convert.ToDouble(r.role_league_tier);

                }
            }


            teamOneAvgConservativeRating /= 5;
            teamOneAvgHeroConservativeRating /= 5;
            teamOneAvgRoleConservativeRating /= 5;

            teamOneAvgConservativeRating = Math.Truncate(teamOneAvgConservativeRating) >= .5 ? Math.Ceiling(teamOneAvgConservativeRating) : Math.Floor(teamOneAvgConservativeRating);

            teamOneAvgHeroConservativeRating = Math.Truncate(teamOneAvgHeroConservativeRating) >= .5 ? Math.Ceiling(teamOneAvgHeroConservativeRating) : Math.Floor(teamOneAvgHeroConservativeRating);

            teamOneAvgRoleConservativeRating = Math.Truncate(teamOneAvgRoleConservativeRating) >= .5 ? Math.Ceiling(teamOneAvgRoleConservativeRating) : Math.Floor(teamOneAvgRoleConservativeRating);


            teamTwoAvgConservativeRating /= 5;
            teamTwoAvgHeroConservativeRating /= 5;
            teamTwoAvgRoleConservativeRating /= 5;

            teamTwoAvgConservativeRating = Math.Truncate(teamTwoAvgConservativeRating) > .5 ? Math.Ceiling(teamTwoAvgConservativeRating) : Math.Floor(teamTwoAvgConservativeRating);

            teamTwoAvgHeroConservativeRating = Math.Truncate(teamTwoAvgHeroConservativeRating) > .5 ? Math.Ceiling(teamTwoAvgHeroConservativeRating) : Math.Floor(teamTwoAvgHeroConservativeRating);

            teamTwoAvgRoleConservativeRating = Math.Truncate(teamTwoAvgRoleConservativeRating) > .5 ? Math.Ceiling(teamTwoAvgRoleConservativeRating) : Math.Floor(teamTwoAvgRoleConservativeRating);

            double teamOneAvgHeroLevel = 0;
            double teamTwoAvgHeroLevel = 0;

            foreach (var r in data.Replay_Player)
            {
                var heroLevel = 0;

                if (r.HeroLevel < 5)
                {
                    heroLevel = 1;
                }
                else if (r.HeroLevel >= 5 && r.HeroLevel < 10)
                {
                    heroLevel = 5;
                }
                else if (r.HeroLevel >= 10 && r.HeroLevel < 15)
                {
                    heroLevel = 10;
                }
                else if (r.HeroLevel >= 15 && r.HeroLevel < 20)
                {
                    heroLevel = 15;
                }
                else if (r.HeroLevel >= 20)
                {
                    heroLevel = r.MasteryTaunt switch
                    {
                            0 => 20,
                            1 => 25,
                            2 => 40,
                            3 => 60,
                            4 => 80,
                            5 => 100,
                            _ => heroLevel
                    };
                }

                if (r.Team == 0)
                {
                    teamOneAvgHeroLevel += heroLevel;

                }
                else
                {
                    teamTwoAvgHeroLevel += heroLevel;

                }
            }

            teamOneAvgHeroLevel /= 5;
            teamTwoAvgHeroLevel /= 5;

            if (teamOneAvgHeroLevel < 5)
            {
                teamOneAvgHeroLevel = 1;
            }
            else if (teamOneAvgHeroLevel >= 5 && teamOneAvgHeroLevel < 10)
            {
                teamOneAvgHeroLevel = 5;
            }
            else if (teamOneAvgHeroLevel >= 10 && teamOneAvgHeroLevel < 15)
            {
                teamOneAvgHeroLevel = 10;
            }
            else if (teamOneAvgHeroLevel >= 15 && teamOneAvgHeroLevel < 20)
            {
                teamOneAvgHeroLevel = 15;
            }
            else if (teamOneAvgHeroLevel >= 20 && teamOneAvgHeroLevel < 25)
            {
                teamOneAvgHeroLevel = 20;
            }
            else if (teamOneAvgHeroLevel >= 25 && teamOneAvgHeroLevel < 40)
            {
                teamOneAvgHeroLevel = 25;
            }
            else if (teamOneAvgHeroLevel >= 40 && teamOneAvgHeroLevel < 60)
            {
                teamOneAvgHeroLevel = 40;
            }
            else if (teamOneAvgHeroLevel >= 60 && teamOneAvgHeroLevel < 80)
            {
                teamOneAvgHeroLevel = 60;
            }
            else if (teamOneAvgHeroLevel >= 80 && teamOneAvgHeroLevel < 100)
            {
                teamOneAvgHeroLevel = 80;
            }
            else if (teamOneAvgHeroLevel >= 100)
            {
                teamOneAvgHeroLevel = 100;
            }


            if (teamTwoAvgHeroLevel < 5)
            {
                teamTwoAvgHeroLevel = 1;
            }
            else if (teamTwoAvgHeroLevel >= 5 && teamTwoAvgHeroLevel < 10)
            {
                teamTwoAvgHeroLevel = 5;
            }
            else if (teamTwoAvgHeroLevel >= 10 && teamTwoAvgHeroLevel < 15)
            {
                teamTwoAvgHeroLevel = 10;
            }
            else if (teamTwoAvgHeroLevel >= 15 && teamTwoAvgHeroLevel < 20)
            {
                teamTwoAvgHeroLevel = 15;
            }
            else if (teamTwoAvgHeroLevel >= 20 && teamTwoAvgHeroLevel < 25)
            {
                teamTwoAvgHeroLevel = 20;
            }
            else if (teamTwoAvgHeroLevel >= 25 && teamTwoAvgHeroLevel < 40)
            {
                teamTwoAvgHeroLevel = 25;
            }
            else if (teamTwoAvgHeroLevel >= 40 && teamTwoAvgHeroLevel < 60)
            {
                teamTwoAvgHeroLevel = 40;
            }
            else if (teamTwoAvgHeroLevel >= 60 && teamTwoAvgHeroLevel < 80)
            {
                teamTwoAvgHeroLevel = 60;
            }
            else if (teamTwoAvgHeroLevel >= 80 && teamTwoAvgHeroLevel < 100)
            {
                teamTwoAvgHeroLevel = 80;
            }
            else if (teamTwoAvgHeroLevel >= 100)
            {
                teamTwoAvgHeroLevel = 100;
            }

            if (data.Bans == null) return;
            {
                for (var i = 0; i < data.Bans.Length; i++)
                {
                    for (var j = 0; j < data.Bans[i].Length; j++)
                    {

                        var value = data.Bans[i][j];


                        using var cmd = conn.CreateCommand();
                        cmd.CommandText =
                                "INSERT INTO global_hero_stats_bans (game_version, game_type, league_tier, hero_league_tier, role_league_tier, game_map, hero_level, region, hero, bans) VALUES (" +
                                "\"" + data.GameVersion + "\"" + "," +
                                "\"" + data.GameType_id + "\"" + ",";

                        if (i == 0)
                        {
                            cmd.CommandText += "\"" + teamOneAvgConservativeRating + "\"" + ",";
                            cmd.CommandText += "\"" + teamOneAvgHeroConservativeRating + "\"" + ",";
                            cmd.CommandText += "\"" + teamOneAvgRoleConservativeRating + "\"" + ",";
                        }
                        else
                        {
                            cmd.CommandText += "\"" + teamTwoAvgConservativeRating + "\"" + ",";
                            cmd.CommandText += "\"" + teamTwoAvgHeroConservativeRating + "\"" + ",";
                            cmd.CommandText += "\"" + teamTwoAvgRoleConservativeRating + "\"" + ",";

                        }

                        cmd.CommandText += "\"" + data.GameMap_id + "\"" + ",";

                        if (i == 0)
                        {
                            cmd.CommandText += "\"" + teamOneAvgHeroLevel + "\"" + ",";
                        }
                        else
                        {
                            cmd.CommandText += "\"" + teamTwoAvgHeroLevel + "\"" + ",";

                        }

                        cmd.CommandText += data.Region + ",";
                        cmd.CommandText += "\"" + value + "\"" + "," +
                                           "\"" + 1 + "\"" + ")";
                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                           "bans = bans + VALUES(bans)";
                        //Console.WriteLine(cmd.CommandText);
                        var reader = cmd.ExecuteReader();
                    }
                }
            }
        }

        private void UpdateMatchups(ReplayData data)
        {
            foreach (var r in data.Replay_Player)
            {
                var winLoss = 0;
                winLoss = r.Winner ? 1 : 0;

                if (r.Score == null) continue;
                var heroLevel = 0;

                if (r.HeroLevel < 5)
                {
                    heroLevel = 1;
                }
                else if (r.HeroLevel >= 5 && r.HeroLevel < 10)
                {
                    heroLevel = 5;
                }
                else if (r.HeroLevel >= 10 && r.HeroLevel < 15)
                {
                    heroLevel = 10;
                }
                else if (r.HeroLevel >= 15 && r.HeroLevel < 20)
                {
                    heroLevel = 15;
                }
                else if (r.HeroLevel >= 20)
                {
                    heroLevel = r.MasteryTaunt switch
                    {
                            0 => 20,
                            1 => 25,
                            2 => 40,
                            3 => 60,
                            4 => 80,
                            5 => 100,
                            _ => heroLevel
                    };
                }

                foreach (var t in data.Replay_Player)
                {
                    if (t.BlizzId == r.BlizzId) continue;

                    if (t.Team == r.Team)
                    {
                        var matchup = new GlobalHeroMatchupsAlly
                        {
                            GameVersion = data.GameVersion,
                            GameType = Convert.ToSByte(data.GameType_id),
                            LeagueTier = Convert.ToSByte(r.player_league_tier),
                            HeroLeagueTier = Convert.ToSByte(r.hero_league_tier),
                            RoleLeagueTier = Convert.ToSByte(r.role_league_tier),
                            GameMap = Convert.ToSByte(data.GameMap_id),
                            HeroLevel = (uint) heroLevel,
                            Hero = Convert.ToSByte(r.Hero_id),
                            Ally = Convert.ToSByte(t.Hero_id),
                            Mirror = (sbyte) r.Mirror,
                            // TODO: Region column doesn't exist in seed data?
                            // Region = data.Region
                            WinLoss = (sbyte) winLoss,
                            GamesPlayed = 1
                        };
                        _context.GlobalHeroMatchupsAlly.Upsert(matchup)
                            .WhenMatched(x => new GlobalHeroMatchupsAlly
                            {
                                GamesPlayed = x.GamesPlayed + matchup.GamesPlayed
                            }).Run();
                    }
                    else
                    {
                        var matchup = new GlobalHeroMatchupsEnemy()
                        {
                            GameVersion = data.GameVersion,
                            GameType = Convert.ToSByte(data.GameType_id),
                            LeagueTier = Convert.ToSByte(r.player_league_tier),
                            HeroLeagueTier = Convert.ToSByte(r.hero_league_tier),
                            RoleLeagueTier = Convert.ToSByte(r.role_league_tier),
                            GameMap = Convert.ToSByte(data.GameMap_id),
                            HeroLevel = (uint) heroLevel,
                            Hero = Convert.ToSByte(r.Hero_id),
                            Enemy = Convert.ToSByte(t.Hero_id),
                            Mirror = (sbyte) r.Mirror,
                            // TODO: Region column doesn't exist in seed data?
                            // Region = data.Region
                            WinLoss = (sbyte) winLoss,
                            GamesPlayed = 1
                        };
                        _context.GlobalHeroMatchupsEnemy.Upsert(matchup)
                            .WhenMatched(x => new GlobalHeroMatchupsEnemy
                            {
                                GamesPlayed = x.GamesPlayed + matchup.GamesPlayed
                            }).Run();
                    }
                }
            }
        }
        private void UpdateGlobalTalentData(ReplayData data, MySqlConnection conn)
        {
            foreach (var t in data.Replay_Player)
            {
                var winLoss = 0;
                winLoss = t.Winner ? 1 : 0;

                if (t.Score == null) continue;
                var heroLevel = 0;

                if (t.HeroLevel < 5)
                {
                    heroLevel = 1;
                }
                else if (t.HeroLevel >= 5 && t.HeroLevel < 10)
                {
                    heroLevel = 5;
                }
                else if (t.HeroLevel >= 10 && t.HeroLevel < 15)
                {
                    heroLevel = 10;
                }
                else if (t.HeroLevel >= 15 && t.HeroLevel < 20)
                {
                    heroLevel = 15;
                }
                else if (t.HeroLevel >= 20)
                {
                    heroLevel = t.MasteryTaunt switch
                    {
                            0 => 20,
                            1 => 25,
                            2 => 40,
                            3 => 60,
                            4 => 80,
                            5 => 100,
                            _ => heroLevel
                    };
                }

                using var cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO global_hero_talents (" +
                                  "game_version, " +
                                  "game_type, " +
                                  "league_tier, " +
                                  "hero_league_tier, " +
                                  "role_league_tier, " +
                                  "game_map, " +
                                  "hero_level, " +
                                  "hero, " +
                                  "mirror, " +
                                  "region, " +
                                  "win_loss, " +
                                  "talent_combination_id, " +
                                  "game_time, " +
                                  "kills, " +
                                  "assists, " +
                                  "takedowns, " +
                                  "deaths, " +
                                  "highest_kill_streak, " +
                                  "hero_damage, " +
                                  "siege_damage, " +
                                  "structure_damage, " +
                                  "minion_damage, " +
                                  "creep_damage, " +
                                  "summon_damage, " +
                                  "time_cc_enemy_heroes, " +
                                  "healing, " +
                                  "self_healing, " +
                                  "damage_taken, " +
                                  "experience_contribution, " +
                                  "town_kills, " +
                                  "time_spent_dead, " +
                                  "merc_camp_captures, " +
                                  "watch_tower_captures, " +
                                  "protection_allies, " +
                                  "silencing_enemies, " +
                                  "rooting_enemies, " +
                                  "stunning_enemies, " +
                                  "clutch_heals, " +
                                  "escapes, " +
                                  "vengeance, " +
                                  "outnumbered_deaths, " +
                                  "teamfight_escapes, " +
                                  "teamfight_healing, " +
                                  "teamfight_damage_taken, " +
                                  "teamfight_hero_damage, " +
                                  "multikill, physical_damage, " +
                                  "spell_damage, " +
                                  "regen_globes, " +
                                  "games_played" +
                                  ") VALUES (" +
                                  "\"" + data.GameVersion + "\"" + ",";
                cmd.CommandText += "\"" + data.GameType_id + "\"" + "," +
                                   "\"" + t.player_league_tier + "\"" + "," +
                                   "\"" + t.hero_league_tier + "\"" + "," +
                                   "\"" + t.role_league_tier + "\"" + "," +
                                   "\"" + data.GameMap_id + "\"" + "," +
                                   "\"" + heroLevel + "\"" + "," +
                                   "\"" + t.Hero_id + "\"" + "," +
                                   "\"" + t.Mirror + "\"" + "," +
                                   "\"" + data.Region + "\"" + "," +
                                   "\"" + winLoss + "\"" + ",";



                if (t.Talents == null || t.Talents.Level_One == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0) + ",";
                }
                else if (t.Talents.Level_Four == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0) + ",";

                }
                else if (t.Talents.Level_Seven == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               t.Talents.Level_Four,
                                               0,
                                               0,
                                               0,
                                               0,
                                               0) + ",";
                }
                else if (t.Talents.Level_Ten == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               t.Talents.Level_Four,
                                               t.Talents.Level_Seven,
                                               0,
                                               0,
                                               0,
                                               0) + ",";
                }
                else if (t.Talents.Level_Thirteen == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               t.Talents.Level_Four,
                                               t.Talents.Level_Seven,
                                               t.Talents.Level_Ten,
                                               0,
                                               0,
                                               0) + ",";

                }
                else if (t.Talents.Level_Sixteen == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               t.Talents.Level_Four,
                                               t.Talents.Level_Seven,
                                               t.Talents.Level_Ten,
                                               t.Talents.Level_Thirteen,
                                               0,
                                               0) + ",";
                }
                else if (t.Talents.Level_Twenty == 0)
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               t.Talents.Level_Four,
                                               t.Talents.Level_Seven,
                                               t.Talents.Level_Ten,
                                               t.Talents.Level_Thirteen,
                                               t.Talents.Level_Sixteen,
                                               0) + ",";
                }
                else
                {
                    cmd.CommandText += GetHeroCombId(
                                               t.Hero_id,
                                               t.Talents.Level_One,
                                               t.Talents.Level_Four,
                                               t.Talents.Level_Seven,
                                               t.Talents.Level_Ten,
                                               t.Talents.Level_Thirteen,
                                               t.Talents.Level_Sixteen,
                                               t.Talents.Level_Twenty) + ",";
                }

                cmd.CommandText += "\"" + data.Length + "\"" + "," +
                                   CheckIfEmpty(t.Score.SoloKills) + "," +
                                   CheckIfEmpty(t.Score.Assists) + "," +
                                   CheckIfEmpty(t.Score.Takedowns) + "," +
                                   CheckIfEmpty(t.Score.Deaths) + "," +
                                   CheckIfEmpty(t.Score.HighestKillStreak) + "," +
                                   CheckIfEmpty(t.Score.HeroDamage) + "," +
                                   CheckIfEmpty(t.Score.SiegeDamage) + "," +
                                   CheckIfEmpty(t.Score.StructureDamage) + "," +
                                   CheckIfEmpty(t.Score.MinionDamage) + "," +
                                   CheckIfEmpty(t.Score.CreepDamage) + "," +
                                   CheckIfEmpty(t.Score.SummonDamage) + "," +
                                   CheckIfEmpty(Convert.ToInt64(t.Score.TimeCCdEnemyHeroes)) + "," +
                                   CheckIfEmpty(t.Score.Healing) + "," +
                                   CheckIfEmpty(t.Score.SelfHealing) + "," +
                                   CheckIfEmpty(t.Score.DamageTaken) + "," +
                                   CheckIfEmpty(t.Score.ExperienceContribution) + "," +
                                   CheckIfEmpty(t.Score.TownKills) + "," +
                                   CheckIfEmpty(Convert.ToInt64(t.Score.TimeSpentDead)) + "," +
                                   CheckIfEmpty(t.Score.MercCampCaptures) + "," +
                                   CheckIfEmpty(t.Score.WatchTowerCaptures) + "," +
                                   CheckIfEmpty(t.Score.ProtectionGivenToAllies) + "," +
                                   CheckIfEmpty(t.Score.TimeSilencingEnemyHeroes) + "," +
                                   CheckIfEmpty(t.Score.TimeRootingEnemyHeroes) + "," +
                                   CheckIfEmpty(t.Score.TimeStunningEnemyHeroes) + "," +
                                   CheckIfEmpty(t.Score.ClutchHealsPerformed) + "," +
                                   CheckIfEmpty(t.Score.EscapesPerformed) + "," +
                                   CheckIfEmpty(t.Score.VengeancesPerformed) + "," +
                                   CheckIfEmpty(t.Score.OutnumberedDeaths) + "," +
                                   CheckIfEmpty(t.Score.TeamfightEscapesPerformed) + "," +
                                   CheckIfEmpty(t.Score.TeamfightHealingDone) + "," +
                                   CheckIfEmpty(t.Score.TeamfightDamageTaken) + "," +
                                   CheckIfEmpty(t.Score.TeamfightHeroDamage) + "," +
                                   CheckIfEmpty(t.Score.Multikill) + "," +
                                   CheckIfEmpty(t.Score.PhysicalDamage) + "," +
                                   CheckIfEmpty(t.Score.SpellDamage) + "," +
                                   CheckIfEmpty(t.Score.RegenGlobes) + "," +
                                   1 + ")";


                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                   "game_time = game_time + VALUES(game_time), " +
                                   "kills = kills + VALUES(kills), " +
                                   "assists = assists + VALUES(assists), " +
                                   "takedowns = takedowns + VALUES(takedowns), " +
                                   "deaths = deaths + VALUES(deaths), " +
                                   "highest_kill_streak = highest_kill_streak + VALUES(highest_kill_streak), " +
                                   "hero_damage = hero_damage + VALUES(hero_damage), " +
                                   "siege_damage = siege_damage + VALUES(siege_damage), " +
                                   "structure_damage = structure_damage + VALUES(structure_damage), " +
                                   "minion_damage = minion_damage + VALUES(minion_damage), " +
                                   "creep_damage = creep_damage + VALUES(creep_damage), " +
                                   "summon_damage = summon_damage + VALUES(summon_damage), " +
                                   "time_cc_enemy_heroes = time_cc_enemy_heroes + VALUES(time_cc_enemy_heroes), " +
                                   "healing = healing + VALUES(healing), " +
                                   "self_healing = self_healing + VALUES(self_healing), " +
                                   "damage_taken = damage_taken + VALUES(damage_taken), " +
                                   "experience_contribution = experience_contribution + VALUES(experience_contribution), " +
                                   "town_kills = town_kills + VALUES(town_kills), " +
                                   "time_spent_dead = time_spent_dead + VALUES(time_spent_dead), " +
                                   "merc_camp_captures = merc_camp_captures + VALUES(merc_camp_captures), " +
                                   "watch_tower_captures = watch_tower_captures + VALUES(watch_tower_captures), " +
                                   "protection_allies = protection_allies + VALUES(protection_allies), " +
                                   "silencing_enemies = silencing_enemies + VALUES(silencing_enemies), " +
                                   "rooting_enemies = rooting_enemies + VALUES(rooting_enemies), " +
                                   "stunning_enemies = stunning_enemies + VALUES(stunning_enemies), " +
                                   "clutch_heals = clutch_heals + VALUES(clutch_heals), " +
                                   "escapes = escapes + VALUES(escapes), " +
                                   "vengeance = vengeance + VALUES(vengeance), " +
                                   "outnumbered_deaths = outnumbered_deaths + VALUES(outnumbered_deaths), " +
                                   "teamfight_escapes = teamfight_escapes + VALUES(teamfight_escapes), " +
                                   "teamfight_healing = teamfight_healing + VALUES(teamfight_healing), " +
                                   "teamfight_damage_taken = teamfight_damage_taken + VALUES(teamfight_damage_taken), " +
                                   "teamfight_hero_damage = teamfight_hero_damage + VALUES(teamfight_hero_damage), " +
                                   "multikill = multikill + VALUES(multikill), " +
                                   "physical_damage = physical_damage + VALUES(physical_damage), " +
                                   "spell_damage = spell_damage + VALUES(spell_damage), " +
                                   "regen_globes = regen_globes + VALUES(regen_globes), " +
                                   "games_played = games_played + VALUES(games_played)";
                //Console.WriteLine(cmd.CommandText);
                var reader = cmd.ExecuteReader();
            }
        }

        private int GetHeroCombId(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            var combId = 0;
            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT talent_combination_id FROM heroesprofile.talent_combinations WHERE " +
                                  "hero = " + hero +
                                  " AND level_one = " + level_one +
                                  " AND level_four = " + level_four +
                                  " AND level_seven = " + level_seven +
                                  " AND level_ten = " + level_ten +
                                  " AND level_thirteen = " + level_thirteen +
                                  " AND level_sixteen = " + level_sixteen +
                                  " AND level_twenty = " + level_twenty;

                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    combId = reader.GetInt32("talent_combination_id");
                }
                if (!reader.HasRows)
                {
                    combId = InsertTalentCombo(hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty);
                }
            }

            return combId;
        }
        private int InsertTalentCombo(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            var combId = 0;

            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO heroesprofile.talent_combinations (hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty) VALUES (" +
                        hero + "," +
                        level_one + "," +
                        level_four + "," +
                        level_seven + "," +
                        level_ten + "," +
                        level_thirteen + "," +
                        level_sixteen + "," +
                        level_twenty +
                        ")";

                    var reader = cmd.ExecuteReader();
                }


                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT talent_combination_id FROM heroesprofile.talent_combinations WHERE " +
                        "hero = " + hero +
                        " AND level_one = " + level_one +
                        " AND level_four = " + level_four +
                        " AND level_seven = " + level_seven +
                        " AND level_ten = " + level_ten +
                        " AND level_thirteen = " + level_thirteen +
                        " AND level_sixteen = " + level_sixteen +
                        " AND level_twenty = " + level_twenty;

                    var reader = cmd.ExecuteReader();

                    while (reader.Read())
                    {
                        combId = reader.GetInt32("talent_combination_id");
                    }
                }
            }

            return combId;
        }
        private void UpdateGlobalTalentDataDetails(ReplayData data, MySqlConnection conn)
        {
            foreach (var t1 in data.Replay_Player)
            {
                var winLoss = 0;
                winLoss = t1.Winner ? 1 : 0;


                if (t1.Talents == null) continue;
                for (var t = 0; t < 7; t++)
                {
                    var level = t switch
                    {
                            0 => "1",
                            1 => "4",
                            2 => "7",
                            3 => "10",
                            4 => "13",
                            5 => "16",
                            6 => "20",
                            _ => ""
                    };

                    if (t1.Score == null) continue;
                    var heroLevel = 0;

                    if (t1.HeroLevel < 5)
                    {
                        heroLevel = 1;
                    }
                    else if (t1.HeroLevel >= 5 && t1.HeroLevel < 10)
                    {
                        heroLevel = 5;
                    }
                    else if (t1.HeroLevel >= 10 && t1.HeroLevel < 15)
                    {
                        heroLevel = 10;
                    }
                    else if (t1.HeroLevel >= 15 && t1.HeroLevel < 20)
                    {
                        heroLevel = 15;
                    }
                    else if (t1.HeroLevel >= 20)
                    {
                        heroLevel = t1.MasteryTaunt switch
                        {
                                0 => 20,
                                1 => 25,
                                2 => 40,
                                3 => 60,
                                4 => 80,
                                5 => 100,
                                _ => heroLevel
                        };
                    }

                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "INSERT INTO global_hero_talents_details (" +
                                      "game_version, " +
                                      "game_type, " +
                                      "league_tier, " +
                                      "hero_league_tier, " +
                                      "role_league_tier," +
                                      "game_map, " +
                                      "hero_level, " +
                                      "hero, " +
                                      "mirror, " +
                                      "region, " +
                                      "win_loss, " +
                                      "level, " +
                                      "talent, " +
                                      "game_time, " +
                                      "kills, " +
                                      "assists, " +
                                      "takedowns, " +
                                      "deaths, " +
                                      "highest_kill_streak, " +
                                      "hero_damage, " +
                                      "siege_damage, " +
                                      "structure_damage, " +
                                      "minion_damage, " +
                                      "creep_damage, " +
                                      "summon_damage, " +
                                      "time_cc_enemy_heroes, " +
                                      "healing, " +
                                      "self_healing, " +
                                      "damage_taken, " +
                                      "experience_contribution, " +
                                      "town_kills, " +
                                      "time_spent_dead, " +
                                      "merc_camp_captures, " +
                                      "watch_tower_captures, " +
                                      "protection_allies, " +
                                      "silencing_enemies, " +
                                      "rooting_enemies, " +
                                      "stunning_enemies, " +
                                      "clutch_heals, " +
                                      "escapes, " +
                                      "vengeance, " +
                                      "outnumbered_deaths, " +
                                      "teamfight_escapes, " +
                                      "teamfight_healing, " +
                                      "teamfight_damage_taken, " +
                                      "teamfight_hero_damage, " +
                                      "multikill, physical_damage, " +
                                      "spell_damage, " +
                                      "regen_globes, " +
                                      "games_played) VALUES (" +
                                      "\"" + data.GameVersion + "\"" + "," +
                                      data.GameType_id + "," +
                                      t1.player_league_tier + "," +
                                      t1.hero_league_tier + "," +
                                      t1.role_league_tier + "," +
                                      data.GameMap_id + "," +
                                      heroLevel + "," +
                                      t1.Hero_id + "," +
                                      t1.Mirror + "," +
                                      data.Region + "," +
                                      "\"" + winLoss + "\"" + "," +
                                      level + ",";

                    switch (t)
                    {
                        case 0:
                            cmd.CommandText += t1.Talents.Level_One + ",";
                            break;
                        case 1:
                            cmd.CommandText += t1.Talents.Level_Four + ",";
                            break;
                        case 2:
                            cmd.CommandText += t1.Talents.Level_Seven + ",";
                            break;
                        case 3:
                            cmd.CommandText += t1.Talents.Level_Ten + ",";
                            break;
                        case 4:
                            cmd.CommandText += t1.Talents.Level_Thirteen + ",";
                            break;
                        case 5:
                            cmd.CommandText += t1.Talents.Level_Sixteen + ",";
                            break;
                        case 6:
                            cmd.CommandText += t1.Talents.Level_Twenty + ",";
                            break;
                    }


                    cmd.CommandText += "\"" + data.Length + "\"" + "," +
                                       CheckIfEmpty(t1.Score.SoloKills) + "," +
                                       CheckIfEmpty(t1.Score.Assists) + "," +
                                       CheckIfEmpty(t1.Score.Takedowns) + "," +
                                       CheckIfEmpty(t1.Score.Deaths) + "," +
                                       CheckIfEmpty(t1.Score.HighestKillStreak) + "," +
                                       CheckIfEmpty(t1.Score.HeroDamage) + "," +
                                       CheckIfEmpty(t1.Score.SiegeDamage) + "," +
                                       CheckIfEmpty(t1.Score.StructureDamage) + "," +
                                       CheckIfEmpty(t1.Score.MinionDamage) + "," +
                                       CheckIfEmpty(t1.Score.CreepDamage) + "," +
                                       CheckIfEmpty(t1.Score.SummonDamage) + "," +
                                       CheckIfEmpty(Convert.ToInt64(t1.Score.TimeCCdEnemyHeroes)) + "," +
                                       CheckIfEmpty(t1.Score.Healing) + "," +
                                       CheckIfEmpty(t1.Score.SelfHealing) + "," +
                                       CheckIfEmpty(t1.Score.DamageTaken) + "," +
                                       CheckIfEmpty(t1.Score.ExperienceContribution) + "," +
                                       CheckIfEmpty(t1.Score.TownKills) + "," +
                                       CheckIfEmpty(Convert.ToInt64(t1.Score.TimeSpentDead)) + "," +
                                       CheckIfEmpty(t1.Score.MercCampCaptures) + "," +
                                       CheckIfEmpty(t1.Score.WatchTowerCaptures) + "," +
                                       CheckIfEmpty(t1.Score.ProtectionGivenToAllies) + "," +
                                       CheckIfEmpty(t1.Score.TimeSilencingEnemyHeroes) + "," +
                                       CheckIfEmpty(t1.Score.TimeRootingEnemyHeroes) + "," +
                                       CheckIfEmpty(t1.Score.TimeStunningEnemyHeroes) + "," +
                                       CheckIfEmpty(t1.Score.ClutchHealsPerformed) + "," +
                                       CheckIfEmpty(t1.Score.EscapesPerformed) + "," +
                                       CheckIfEmpty(t1.Score.VengeancesPerformed) + "," +
                                       CheckIfEmpty(t1.Score.OutnumberedDeaths) + "," +
                                       CheckIfEmpty(t1.Score.TeamfightEscapesPerformed) + "," +
                                       CheckIfEmpty(t1.Score.TeamfightHealingDone) + "," +
                                       CheckIfEmpty(t1.Score.TeamfightDamageTaken) + "," +
                                       CheckIfEmpty(t1.Score.TeamfightHeroDamage) + "," +
                                       CheckIfEmpty(t1.Score.Multikill) + "," +
                                       CheckIfEmpty(t1.Score.PhysicalDamage) + "," +
                                       CheckIfEmpty(t1.Score.SpellDamage) + "," +
                                       CheckIfEmpty(t1.Score.RegenGlobes) + "," +
                                       1 + ")";


                    cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                       "game_time = game_time + VALUES(game_time), " +
                                       "kills = kills + VALUES(kills), " +
                                       "assists = assists + VALUES(assists), " +
                                       "takedowns = takedowns + VALUES(takedowns), " +
                                       "deaths = deaths + VALUES(deaths), " +
                                       "highest_kill_streak = highest_kill_streak + VALUES(highest_kill_streak), " +
                                       "hero_damage = hero_damage + VALUES(hero_damage), " +
                                       "siege_damage = siege_damage + VALUES(siege_damage), " +
                                       "structure_damage = structure_damage + VALUES(structure_damage), " +
                                       "minion_damage = minion_damage + VALUES(minion_damage), " +
                                       "creep_damage = creep_damage + VALUES(creep_damage), " +
                                       "summon_damage = summon_damage + VALUES(summon_damage), " +
                                       "time_cc_enemy_heroes = time_cc_enemy_heroes + VALUES(time_cc_enemy_heroes), " +
                                       "healing = healing + VALUES(healing), " +
                                       "self_healing = self_healing + VALUES(self_healing), " +
                                       "damage_taken = damage_taken + VALUES(damage_taken), " +
                                       "experience_contribution = experience_contribution + VALUES(experience_contribution), " +
                                       "town_kills = town_kills + VALUES(town_kills), " +
                                       "time_spent_dead = time_spent_dead + VALUES(time_spent_dead), " +
                                       "merc_camp_captures = merc_camp_captures + VALUES(merc_camp_captures), " +
                                       "watch_tower_captures = watch_tower_captures + VALUES(watch_tower_captures), " +
                                       "protection_allies = protection_allies + VALUES(protection_allies), " +
                                       "silencing_enemies = silencing_enemies + VALUES(silencing_enemies), " +
                                       "rooting_enemies = rooting_enemies + VALUES(rooting_enemies), " +
                                       "stunning_enemies = stunning_enemies + VALUES(stunning_enemies), " +
                                       "clutch_heals = clutch_heals + VALUES(clutch_heals), " +
                                       "escapes = escapes + VALUES(escapes), " +
                                       "vengeance = vengeance + VALUES(vengeance), " +
                                       "outnumbered_deaths = outnumbered_deaths + VALUES(outnumbered_deaths), " +
                                       "teamfight_escapes = teamfight_escapes + VALUES(teamfight_escapes), " +
                                       "teamfight_healing = teamfight_healing + VALUES(teamfight_healing), " +
                                       "teamfight_damage_taken = teamfight_damage_taken + VALUES(teamfight_damage_taken), " +
                                       "teamfight_hero_damage = teamfight_hero_damage + VALUES(teamfight_hero_damage), " +
                                       "multikill = multikill + VALUES(multikill), " +
                                       "physical_damage = physical_damage + VALUES(physical_damage), " +
                                       "spell_damage = spell_damage + VALUES(spell_damage), " +
                                       "regen_globes = regen_globes + VALUES(regen_globes), " +
                                       "games_played = games_played + VALUES(games_played)";
                    //Console.WriteLine(cmd.CommandText);
                    var reader = cmd.ExecuteReader();
                }
            }
        }

        private void UpdateDeathwingData(ReplayData data, MySqlConnection conn)
        {
            //TODO: The deathwing_data table doesn't exist in the seeded dbs?

            foreach (var player in data.Replay_Player)
            {
                var buildingsDestroyed = 0;
                var villagersSlain = 0;
                var raidersKilled = 0;
                var deathwingKilled = 0;
                if (player.Hero_id != "89") continue;
                buildingsDestroyed += Convert.ToInt32(player.Score.SiegeDamage);
                raidersKilled += Convert.ToInt32(player.Score.Takedowns);
                villagersSlain += (Convert.ToInt32(player.Score.CreepDamage) + Convert.ToInt32(player.Score.MinionDamage) + Convert.ToInt32(player.Score.SummonDamage));
                deathwingKilled += Convert.ToInt32(player.Score.Deaths);

                villagersSlain /= 812;
                buildingsDestroyed /= 12900;

                using var cmd = conn.CreateCommand();
                cmd.CommandText = "INSERT INTO deathwing_data (game_type, buildings_destroyed, villagers_slain, raiders_killed, deathwing_killed) VALUES(" +
                                  data.GameType_id + "," +
                                  buildingsDestroyed + "," +
                                  villagersSlain + "," +
                                  raidersKilled + "," +
                                  deathwingKilled + ")";
                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                   "buildings_destroyed = buildings_destroyed + VALUES(buildings_destroyed)," +
                                   "villagers_slain = villagers_slain + VALUES(villagers_slain)," +
                                   "raiders_killed = raiders_killed + VALUES(raiders_killed)," +
                                   "deathwing_killed = deathwing_killed + VALUES(deathwing_killed)";

                cmd.CommandTimeout = 0;
                //Console.WriteLine(cmd.CommandText);
                var reader = cmd.ExecuteReader();
            }
        }

        private static string CheckIfEmpty(long? value)
        {
            return value == null ? "NULL" : value.ToString();
        }
    }
}
