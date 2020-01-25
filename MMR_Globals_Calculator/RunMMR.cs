using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
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
                        UpdateGlobalTalentData(data);
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

            var replay = _context.Replay.FirstOrDefault(x => x.ReplayId == replayId);

            if (replay == null) return data;
            {
                //Replay level items
                data.Id = replay.ReplayId;
                data.GameType_id = replay.GameType.ToString();
                data.Region = replay.Region;
                data.GameDate = replay.GameDate;
                data.Length = replay.GameLength;
                data.GameVersion = replay.GameVersion;
                data.Bans = GetBans(data.Id);
                data.GameMap_id = replay.GameMap.ToString();

                //Player level items
                var replayPlayers = _context.Player.Where(x => x.ReplayId == replayId).OrderBy(x => x.Team).ToList();
                foreach (var player in replayPlayers)
                {
                    //Queries
                    var replayTalent = _context.Talents.FirstOrDefault(x => x.ReplayId == replayId
                                                                         && x.Battletag.ToString() ==
                                                                            player.Battletag);
                    var score = _context.Scores
                                        .FirstOrDefault(x => x.ReplayId == replayId
                                                          && x.Battletag == player.Battletag);

                    //Assignments
                    var replayPlayer = new ReplayPlayer
                    {
                            Hero_id = player.Hero.ToString(),
                            Hero = heroIds[player.Hero.ToString()],
                            Role = roles[player.Hero.ToString()],
                            BlizzId = player.BlizzId,
                            Winner = player.Winner == 1,
                            HeroLevel = player.HeroLevel,
                            MasteryTaunt = player.MasteryTaunt ?? 0,
                            Mirror = 0,
                            Team = player.Team,
                            Talents = new Talents
                            {
                                    Level_One = replayTalent?.LevelOne ?? 0,
                                    Level_Four = replayTalent?.LevelFour ?? 0,
                                    Level_Seven = replayTalent?.LevelSeven ?? 0,
                                    Level_Ten = replayTalent?.LevelTen ?? 0,
                                    Level_Thirteen = replayTalent?.LevelThirteen ?? 0,
                                    Level_Sixteen = replayTalent?.LevelSixteen ?? 0,
                                    Level_Twenty = replayTalent?.LevelTwenty ?? 0
                            },
                            Score = new Score
                            {
                                    SoloKills = score?.Kills ?? 0,
                                    Assists = score?.Assists ?? 0,
                                    Takedowns = score?.Takedowns ?? 0,
                                    Deaths = score?.Deaths ?? 0,
                                    HighestKillStreak = score?.HighestKillStreak ?? 0,
                                    HeroDamage = score?.HeroDamage ?? 0,
                                    SiegeDamage = score?.SiegeDamage ?? 0,
                                    StructureDamage = score?.StructureDamage ?? 0,
                                    MinionDamage = score?.MinionDamage ?? 0,
                                    CreepDamage = score?.CreepDamage ?? 0,
                                    SummonDamage = score?.SummonDamage ?? 0,
                                    TimeCCdEnemyHeroes = score?.TimeCcEnemyHeroes ?? 0,
                                    Healing = score?.Healing ?? 0,
                                    SelfHealing = score?.SelfHealing ?? 0,
                                    DamageTaken = score?.DamageTaken ?? 0,
                                    ExperienceContribution = score?.ExperienceContribution ?? 0,
                                    TownKills = score?.TownKills ?? 0,
                                    TimeSpentDead = score?.TimeSpentDead ?? 0,
                                    MercCampCaptures = score?.MercCampCaptures ?? 0,
                                    WatchTowerCaptures = score?.WatchTowerCaptures ?? 0,
                                    ProtectionGivenToAllies = score?.ProtectionAllies ?? 0,
                                    TimeSilencingEnemyHeroes = score?.SilencingEnemies ?? 0,
                                    TimeRootingEnemyHeroes = score?.RootingEnemies ?? 0,
                                    TimeStunningEnemyHeroes = score?.StunningEnemies ?? 0,
                                    ClutchHealsPerformed = score?.ClutchHeals ?? 0,
                                    EscapesPerformed = score?.Escapes ?? 0,
                                    VengeancesPerformed = score?.Vengeance ?? 0,
                                    OutnumberedDeaths = score?.OutnumberedDeaths ?? 0,
                                    TeamfightEscapesPerformed = score?.TeamfightEscapes ?? 0,
                                    TeamfightHealingDone = score?.TeamfightHealing ?? 0,
                                    TeamfightDamageTaken = score?.TeamfightDamageTaken ?? 0,
                                    TeamfightHeroDamage = score?.TeamfightHeroDamage ?? 0,
                                    Multikill = score?.Multikill ?? 0,
                                    PhysicalDamage = score?.PhysicalDamage ?? 0,
                                    SpellDamage = score?.SpellDamage ?? 0,
                                    RegenGlobes = score?.RegenGlobes ?? 0
                            }
                    };
                    players[playerCounter] = replayPlayer;
                    playerCounter++;
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
                    _context.Replay
                            .Where(x => x.ReplayId == replayId)
                            .Update(x => new Replay
                            {
                                    MmrRan = 1
                            });
                }
            }

            return data;
        }

        private int[][] GetBans(long replayId)
        {
            var bans = new int[2][];
            bans[0] = new int[3];
            bans[1] = new int[3];

            var teamOneCounter = 0;
            var teamTwoCounter = 0;

            var replayBans = _context.ReplayBans.Where(x => x.ReplayId == replayId).ToList();

            foreach (var replayBan in replayBans)
            {
                if (replayBan.Team == 0)
                {
                    bans[0][teamOneCounter] = (int) replayBan.Hero;
                    teamOneCounter++;
                }
                else
                {
                    bans[1][teamTwoCounter] = (int) replayBan.Hero;
                    teamTwoCounter++;
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
            var leagueBreakdown = _context.LeagueBreakdowns.Where(x => x.TypeRoleHero == mmrId
                                                                       && x.GameType == Convert.ToSByte(gameTypeId)
                                                                       && x.MinMmr <= mmr)
                .OrderByDescending(x => x.MinMmr)
                .Take(1)
                .FirstOrDefault();

            return leagueBreakdown == null ? "1" : leagueBreakdown.LeagueTier.ToString();
        }

        private void UpdatePlayerMmr(ReplayData data)
        {
            foreach (var r in data.Replay_Player)
            {
                _context.Player
                    .Where(x => x.ReplayId == data.Id
                                && x.BlizzId == r.BlizzId)
                    .Update(x => new Player
                    {
                        PlayerConservativeRating = r.player_conservative_rating,
                        PlayerMean = r.player_mean,
                        PlayerStandardDeviation = r.player_standard_deviation,
                        HeroConservativeRating = r.hero_conservative_rating,
                        HeroMean = r.hero_mean,
                        HeroStandardDeviation = r.hero_standard_deviation,
                        RoleConservativeRating = r.role_conservative_rating,
                        RoleMean = r.role_mean,
                        RoleStandardDeviation = r.role_standard_deviation,
                        MmrDateParsed = DateTime.Now
                    });
            }
        }

        private void SaveMasterMmrData(ReplayData data, Dictionary<string, uint> mmrTypeIdsDict,
            Dictionary<string, string> roles)
        {
            using var conn = new MySqlConnection(_connectionString);
            conn.Open();

            foreach (var r in data.Replay_Player)
            {
                uint win;
                uint loss;

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

                var masterMmrDataPlayer = new MasterMmrData
                {
                    TypeValue = (int) mmrTypeIdsDict["player"],
                    GameType = Convert.ToByte(data.GameType_id),
                    BlizzId = (uint) r.BlizzId,
                    Region = (byte) data.Region,
                    ConservativeRating = r.player_conservative_rating,
                    Mean = r.player_mean,
                    StandardDeviation = r.player_standard_deviation,
                    Win = win,
                    Loss = loss
                };

                _context.MasterMmrData.Upsert(masterMmrDataPlayer)
                    .WhenMatched(x => new MasterMmrData
                    {
                        TypeValue = masterMmrDataPlayer.TypeValue,
                        GameType = masterMmrDataPlayer.GameType,
                        BlizzId = masterMmrDataPlayer.BlizzId,
                        Region = masterMmrDataPlayer.Region,
                        ConservativeRating = masterMmrDataPlayer.ConservativeRating,
                        Mean = masterMmrDataPlayer.Mean,
                        StandardDeviation = masterMmrDataPlayer.StandardDeviation,
                        Win = masterMmrDataPlayer.Win,
                        Loss = masterMmrDataPlayer.Loss,
                    }).Run();

                var masterMmrDataRole = new MasterMmrData
                {
                    TypeValue = (int) mmrTypeIdsDict[roles[r.Hero]],
                    GameType = Convert.ToByte(data.GameType_id),
                    BlizzId = (uint) r.BlizzId,
                    Region = (byte) data.Region,
                    ConservativeRating = r.role_conservative_rating,
                    Mean = r.role_mean,
                    StandardDeviation = r.role_standard_deviation,
                    Win = win,
                    Loss = loss
                };

                _context.MasterMmrData.Upsert(masterMmrDataRole)
                    .WhenMatched(x => new MasterMmrData
                    {
                        TypeValue = masterMmrDataRole.TypeValue,
                        GameType = masterMmrDataRole.GameType,
                        BlizzId = masterMmrDataRole.BlizzId,
                        Region = masterMmrDataRole.Region,
                        ConservativeRating = masterMmrDataRole.ConservativeRating,
                        Mean = masterMmrDataRole.Mean,
                        StandardDeviation = masterMmrDataRole.StandardDeviation,
                        Win = masterMmrDataRole.Win,
                        Loss = masterMmrDataRole.Loss,
                    }).Run();

                var masterMmrDataHero = new MasterMmrData
                {
                    TypeValue = Convert.ToInt32(r.Hero_id),
                    GameType = Convert.ToByte(data.GameType_id),
                    BlizzId = (uint) r.BlizzId,
                    Region = (byte) data.Region,
                    ConservativeRating = r.hero_conservative_rating,
                    Mean = r.hero_mean,
                    StandardDeviation = r.hero_standard_deviation,
                    Win = win,
                    Loss = loss
                };

                _context.MasterMmrData.Upsert(masterMmrDataHero)
                    .WhenMatched(x => new MasterMmrData
                    {
                        TypeValue = masterMmrDataHero.TypeValue,
                        GameType = masterMmrDataHero.GameType,
                        BlizzId = masterMmrDataHero.BlizzId,
                        Region = masterMmrDataHero.Region,
                        ConservativeRating = masterMmrDataHero.ConservativeRating,
                        Mean = masterMmrDataHero.Mean,
                        StandardDeviation = masterMmrDataHero.StandardDeviation,
                        Win = masterMmrDataHero.Win,
                        Loss = masterMmrDataHero.Loss,
                    }).Run();

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

                var globalHeroStats = new GlobalHeroStats
                {
                        GameVersion = data.GameVersion,
                        GameType = Convert.ToSByte(data.GameType_id),
                        LeagueTier = Convert.ToSByte(player.player_league_tier),
                        HeroLeagueTier = Convert.ToSByte(player.hero_league_tier),
                        RoleLeagueTier = Convert.ToSByte(player.role_league_tier),
                        GameMap = Convert.ToSByte(data.GameMap_id),
                        HeroLevel = (uint) heroLevel,
                        Hero = Convert.ToSByte(player.Hero_id),
                        Mirror = (sbyte) player.Mirror,
                        // TODO: Region doesn't exist in the db?
                        // Region = data.Region
                        WinLoss = (sbyte) winLoss,
                        GameTime = (uint?) data.Length,
                        Kills = (uint?) player.Score.SoloKills,
                        Assists = (uint?) player.Score.Assists,
                        Takedowns = (uint?) player.Score.Takedowns,
                        Deaths = (uint?) player.Score.Deaths,
                        HighestKillStreak = (uint?) player.Score.HighestKillStreak,
                        HeroDamage = (uint?) player.Score.HeroDamage,
                        SiegeDamage = (uint?) player.Score.SiegeDamage,
                        StructureDamage = (uint?) player.Score.StructureDamage,
                        MinionDamage = (uint?) player.Score.MinionDamage,
                        CreepDamage = (uint?) player.Score.CreepDamage,
                        SummonDamage = (uint?) player.Score.SummonDamage,
                        TimeCcEnemyHeroes = (uint?) player.Score.TimeCCdEnemyHeroes,
                        Healing = (uint?) player.Score.Healing,
                        SelfHealing = (uint?) player.Score.SelfHealing,
                        DamageTaken = (uint?) player.Score.DamageTaken,
                        ExperienceContribution = (uint?) player.Score.ExperienceContribution,
                        TownKills = (uint?) player.Score.TownKills,
                        TimeSpentDead = (uint?) player.Score.TimeSpentDead,
                        MercCampCaptures = (uint?) player.Score.MercCampCaptures,
                        WatchTowerCaptures = (uint?) player.Score.WatchTowerCaptures,
                        ProtectionAllies = (uint?) player.Score.ProtectionGivenToAllies,
                        SilencingEnemies = (uint?) player.Score.TimeSilencingEnemyHeroes,
                        RootingEnemies = (uint?) player.Score.TimeRootingEnemyHeroes,
                        StunningEnemies = (uint?) player.Score.TimeStunningEnemyHeroes,
                        ClutchHeals = (uint?) player.Score.ClutchHealsPerformed,
                        Escapes = (uint?) player.Score.EscapesPerformed,
                        Vengeance = (uint?) player.Score.VengeancesPerformed,
                        OutnumberedDeaths = (uint?) player.Score.OutnumberedDeaths,
                        TeamfightEscapes = (uint?) player.Score.TeamfightEscapesPerformed,
                        TeamfightHealing = (uint?) player.Score.TeamfightHealingDone,
                        TeamfightDamageTaken = (uint?) player.Score.TeamfightDamageTaken,
                        TeamfightHeroDamage = (uint?) player.Score.TeamfightHeroDamage,
                        Multikill = (uint?) player.Score.Multikill,
                        PhysicalDamage = (uint?) player.Score.PhysicalDamage,
                        SpellDamage = (uint?) player.Score.SpellDamage,
                        RegenGlobes = (int?) player.Score.RegenGlobes,
                        GamesPlayed = 1
                };

                _context.GlobalHeroStats.Upsert(globalHeroStats)
                        .WhenMatched(x => new GlobalHeroStats
                        {
                                GameTime = x.GameTime + globalHeroStats.GameTime,
                                Kills = x.Kills + globalHeroStats.Kills,
                                Assists = x.Assists + globalHeroStats.Assists,
                                Takedowns = x.Takedowns + globalHeroStats.Takedowns,
                                Deaths = x.Deaths + globalHeroStats.Deaths,
                                HighestKillStreak = x.HighestKillStreak + globalHeroStats.HighestKillStreak,
                                HeroDamage = x.HeroDamage + globalHeroStats.HeroDamage,
                                SiegeDamage = x.SiegeDamage + globalHeroStats.SiegeDamage,
                                StructureDamage = x.StructureDamage + globalHeroStats.StructureDamage,
                                MinionDamage = x.MinionDamage + globalHeroStats.MinionDamage,
                                CreepDamage = x.CreepDamage + globalHeroStats.CreepDamage,
                                SummonDamage = x.SummonDamage + globalHeroStats.SummonDamage,
                                TimeCcEnemyHeroes = x.TimeCcEnemyHeroes + globalHeroStats.TimeCcEnemyHeroes,
                                Healing = x.Healing + globalHeroStats.Healing,
                                SelfHealing = x.SelfHealing + globalHeroStats.SelfHealing,
                                DamageTaken = x.DamageTaken + globalHeroStats.DamageTaken,
                                ExperienceContribution =
                                        x.ExperienceContribution + globalHeroStats.ExperienceContribution,
                                TownKills = x.TownKills + globalHeroStats.TownKills,
                                TimeSpentDead = x.TimeSpentDead + globalHeroStats.TimeSpentDead,
                                MercCampCaptures = x.MercCampCaptures + globalHeroStats.MercCampCaptures,
                                WatchTowerCaptures = x.WatchTowerCaptures + globalHeroStats.WatchTowerCaptures,
                                ProtectionAllies = x.ProtectionAllies + globalHeroStats.ProtectionAllies,
                                SilencingEnemies = x.SilencingEnemies + globalHeroStats.SilencingEnemies,
                                RootingEnemies = x.RootingEnemies + globalHeroStats.RootingEnemies,
                                StunningEnemies = x.StunningEnemies + globalHeroStats.StunningEnemies,
                                ClutchHeals = x.ClutchHeals + globalHeroStats.ClutchHeals,
                                Escapes = x.Escapes + globalHeroStats.Escapes,
                                Vengeance = x.Vengeance + globalHeroStats.Vengeance,
                                OutnumberedDeaths = x.OutnumberedDeaths + globalHeroStats.OutnumberedDeaths,
                                TeamfightEscapes = x.TeamfightEscapes + globalHeroStats.TeamfightEscapes,
                                TeamfightHealing = x.TeamfightHealing + globalHeroStats.TeamfightHealing,
                                TeamfightDamageTaken = x.TeamfightDamageTaken + globalHeroStats.TeamfightDamageTaken,
                                TeamfightHeroDamage = x.TeamfightHeroDamage + globalHeroStats.TeamfightHeroDamage,
                                Multikill = x.Multikill + globalHeroStats.Multikill,
                                PhysicalDamage = x.PhysicalDamage + globalHeroStats.PhysicalDamage,
                                SpellDamage = x.SpellDamage + globalHeroStats.SpellDamage,
                                RegenGlobes = x.RegenGlobes + globalHeroStats.RegenGlobes,
                                GamesPlayed = x.GamesPlayed + globalHeroStats.GamesPlayed,
                        }).Run();
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

            teamOneAvgConservativeRating = Math.Truncate(teamOneAvgConservativeRating) >= .5
                    ? Math.Ceiling(teamOneAvgConservativeRating)
                    : Math.Floor(teamOneAvgConservativeRating);

            teamOneAvgHeroConservativeRating = Math.Truncate(teamOneAvgHeroConservativeRating) >= .5
                    ? Math.Ceiling(teamOneAvgHeroConservativeRating)
                    : Math.Floor(teamOneAvgHeroConservativeRating);

            teamOneAvgRoleConservativeRating = Math.Truncate(teamOneAvgRoleConservativeRating) >= .5
                    ? Math.Ceiling(teamOneAvgRoleConservativeRating)
                    : Math.Floor(teamOneAvgRoleConservativeRating);


            teamTwoAvgConservativeRating /= 5;
            teamTwoAvgHeroConservativeRating /= 5;
            teamTwoAvgRoleConservativeRating /= 5;

            teamTwoAvgConservativeRating = Math.Truncate(teamTwoAvgConservativeRating) > .5
                    ? Math.Ceiling(teamTwoAvgConservativeRating)
                    : Math.Floor(teamTwoAvgConservativeRating);

            teamTwoAvgHeroConservativeRating = Math.Truncate(teamTwoAvgHeroConservativeRating) > .5
                    ? Math.Ceiling(teamTwoAvgHeroConservativeRating)
                    : Math.Floor(teamTwoAvgHeroConservativeRating);

            teamTwoAvgRoleConservativeRating = Math.Truncate(teamTwoAvgRoleConservativeRating) > .5
                    ? Math.Ceiling(teamTwoAvgRoleConservativeRating)
                    : Math.Floor(teamTwoAvgRoleConservativeRating);

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

                        var globalHeroStatsBans = new GlobalHeroStatsBans
                        {
                                GameVersion = data.GameVersion,
                                GameType = Convert.ToSByte(data.GameType_id),
                                LeagueTier =
                                        (sbyte) (i == 0 ? teamOneAvgConservativeRating : teamTwoAvgConservativeRating),
                                HeroLeagueTier = (sbyte) (i == 0
                                        ? teamOneAvgHeroConservativeRating
                                        : teamTwoAvgHeroConservativeRating),
                                RoleLeagueTier = (sbyte) (i == 0
                                        ? teamOneAvgRoleConservativeRating
                                        : teamTwoAvgRoleConservativeRating),
                                GameMap = Convert.ToSByte(data.GameMap_id),
                                HeroLevel = (sbyte) (i == 0 ? teamOneAvgHeroLevel : teamTwoAvgHeroLevel),
                                // TODO : Region doesn't exist in the db?
                                // Region = data.Region
                                Hero = (sbyte) value,
                                Bans = 1
                        };
                        _context.GlobalHeroStatsBans.Upsert(globalHeroStatsBans)
                                .WhenMatched(x => new GlobalHeroStatsBans
                                {
                                        Bans = x.Bans + globalHeroStatsBans.Bans
                                }).Run();
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
                        var matchup = new GlobalHeroMatchupsEnemy
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

        private void UpdateGlobalTalentData(ReplayData data)
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

                int talentComboId;
                if (player.Talents == null)
                {
                    talentComboId = GetOrInsertHeroTalentComboId(player.Hero_id, 0, 0, 0, 0, 0, 0, 0);
                }
                else
                {
                    talentComboId = GetOrInsertHeroTalentComboId(player.Hero_id,
                        player.Talents.Level_One,
                        player.Talents.Level_Four,
                        player.Talents.Level_Seven,
                        player.Talents.Level_Ten,
                        player.Talents.Level_Thirteen,
                        player.Talents.Level_Sixteen,
                        player.Talents.Level_Twenty);
                }

                var talent = new GlobalHeroTalents
                {
                    GameVersion = data.GameVersion,
                    GameType = Convert.ToSByte(data.GameType_id),
                    LeagueTier = Convert.ToSByte(player.player_league_tier),
                    HeroLeagueTier = Convert.ToSByte(player.hero_league_tier),
                    RoleLeagueTier = Convert.ToSByte(player.role_league_tier),
                    GameMap = Convert.ToSByte(data.GameMap_id),
                    HeroLevel = (uint) heroLevel,
                    Hero = Convert.ToSByte(player.Hero_id),
                    Mirror = (sbyte) player.Mirror,
                    //TODO: Region column doesn't exist in db
                    // Region = data.Region,
                    WinLoss = (sbyte) winLoss,
                    TalentCombinationId = talentComboId,
                    GameTime = (int) data.Length,
                    Kills = (int) (player.Score.SoloKills ?? 0),
                    Assists = (int) (player.Score.Assists ?? 0),
                    Takedowns = (int) (player.Score.Takedowns ?? 0),
                    Deaths = (int) (player.Score.Deaths ?? 0),
                    HighestKillStreak = (int) (player.Score.HighestKillStreak ?? 0),
                    HeroDamage = (int) (player.Score.HeroDamage ?? 0),
                    SiegeDamage = (int) (player.Score.SiegeDamage ?? 0),
                    StructureDamage = (int) (player.Score.StructureDamage ?? 0),
                    MinionDamage = (int) (player.Score.MinionDamage ?? 0),
                    CreepDamage = (int) (player.Score.CreepDamage ?? 0),
                    SummonDamage = (int) (player.Score.SummonDamage ?? 0),
                    TimeCcEnemyHeroes = (int) (player.Score.TimeCCdEnemyHeroes ?? 0),
                    Healing = (int) (player.Score.Healing ?? 0),
                    SelfHealing = (int) (player.Score.SelfHealing ?? 0),
                    DamageTaken = (int) (player.Score.DamageTaken ?? 0),
                    ExperienceContribution = (int) (player.Score.ExperienceContribution ?? 0),
                    TownKills = (int) (player.Score.TownKills ?? 0),
                    TimeSpentDead = (int) (player.Score.TimeSpentDead ?? 0),
                    MercCampCaptures = (int) (player.Score.MercCampCaptures ?? 0),
                    WatchTowerCaptures = (int) (player.Score.WatchTowerCaptures ?? 0),
                    ProtectionAllies = (int) (player.Score.ProtectionGivenToAllies ?? 0),
                    SilencingEnemies = (int) (player.Score.TimeSilencingEnemyHeroes ?? 0),
                    RootingEnemies = (int) (player.Score.TimeRootingEnemyHeroes ?? 0),
                    StunningEnemies = (int) (player.Score.TimeStunningEnemyHeroes ?? 0),
                    ClutchHeals = (int) (player.Score.ClutchHealsPerformed ?? 0),
                    Escapes = (int) (player.Score.EscapesPerformed ?? 0),
                    Vengeance = (int) (player.Score.VengeancesPerformed ?? 0),
                    OutnumberedDeaths = (int) (player.Score.OutnumberedDeaths ?? 0),
                    TeamfightEscapes = (int) (player.Score.TeamfightEscapesPerformed ?? 0),
                    TeamfightHealing = (int) (player.Score.TeamfightHealingDone ?? 0),
                    TeamfightDamageTaken = (int) (player.Score.TeamfightDamageTaken ?? 0),
                    TeamfightHeroDamage = (int) (player.Score.TeamfightHeroDamage ?? 0),
                    Multikill = (int) (player.Score.Multikill ?? 0),
                    PhysicalDamage = (int) (player.Score.PhysicalDamage ?? 0),
                    SpellDamage = (int) (player.Score.SpellDamage ?? 0),
                    RegenGlobes = (int) (player.Score.RegenGlobes ?? 0),
                    GamesPlayed = 1,
                };

                _context.GlobalHeroTalents.Upsert(talent)
                    .WhenMatched(x => new GlobalHeroTalents
                    {
                        GameTime = x.GameTime + talent.GamesPlayed,
                        Kills = x.Kills + talent.Kills,
                        Assists = x.Assists + talent.Assists,
                        Takedowns = x.Takedowns + talent.Takedowns,
                        Deaths = x.Deaths + talent.Deaths,
                        HighestKillStreak = x.HighestKillStreak + talent.HighestKillStreak,
                        HeroDamage = x.HeroDamage + talent.HeroDamage,
                        SiegeDamage = x.SiegeDamage + talent.SiegeDamage,
                        StructureDamage = x.StructureDamage + talent.StructureDamage,
                        MinionDamage = x.MinionDamage + talent.MinionDamage,
                        CreepDamage = x.CreepDamage + talent.CreepDamage,
                        SummonDamage = x.SummonDamage + talent.SummonDamage,
                        TimeCcEnemyHeroes = x.TimeCcEnemyHeroes + talent.TimeCcEnemyHeroes,
                        Healing = x.Healing + talent.Healing,
                        SelfHealing = x.SelfHealing + talent.SelfHealing,
                        DamageTaken = x.DamageTaken + talent.DamageTaken,
                        ExperienceContribution = x.ExperienceContribution + talent.ExperienceContribution,
                        TownKills = x.TownKills + talent.TownKills,
                        TimeSpentDead = x.TimeSpentDead + talent.TimeSpentDead,
                        MercCampCaptures = x.MercCampCaptures + talent.MercCampCaptures,
                        WatchTowerCaptures = x.WatchTowerCaptures + talent.WatchTowerCaptures,
                        ProtectionAllies = x.ProtectionAllies + talent.ProtectionAllies,
                        SilencingEnemies = x.SilencingEnemies + talent.SilencingEnemies,
                        RootingEnemies = x.RootingEnemies + talent.RootingEnemies,
                        StunningEnemies = x.StunningEnemies + talent.StunningEnemies,
                        ClutchHeals = x.ClutchHeals + talent.ClutchHeals,
                        Escapes = x.Escapes + talent.Escapes,
                        Vengeance = x.Vengeance + talent.Vengeance,
                        OutnumberedDeaths = x.OutnumberedDeaths + talent.OutnumberedDeaths,
                        TeamfightEscapes = x.TeamfightEscapes + talent.TeamfightEscapes,
                        TeamfightHealing = x.TeamfightHealing + talent.TeamfightHealing,
                        TeamfightDamageTaken = x.TeamfightDamageTaken + talent.TeamfightDamageTaken,
                        TeamfightHeroDamage = x.TeamfightHeroDamage + talent.TeamfightHeroDamage,
                        Multikill = x.Multikill + talent.Multikill,
                        PhysicalDamage = x.PhysicalDamage + talent.PhysicalDamage,
                        SpellDamage = x.SpellDamage + talent.SpellDamage,
                        RegenGlobes = x.RegenGlobes + talent.RegenGlobes,
                        GamesPlayed = x.GamesPlayed + talent.GamesPlayed,
                    }).Run();
            }
        }

        private int GetOrInsertHeroTalentComboId(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            var talentCombo = _context.TalentCombinations.FirstOrDefault(x =>
                x.Hero == Convert.ToInt32(hero)
                && x.LevelOne == level_one
                && x.LevelFour == level_four
                && x.LevelSeven == level_seven
                && x.LevelTen == level_ten
                && x.LevelThirteen == level_thirteen
                && x.LevelSixteen == level_sixteen
                && x.LevelTwenty == level_twenty);

            var combId = talentCombo?.TalentCombinationId ?? InsertTalentCombo(hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty);

            return combId;
        }

        private int InsertTalentCombo(string hero, int level_one, int level_four, int level_seven, int level_ten,
            int level_thirteen, int level_sixteen, int level_twenty)
        {
            var combo = new TalentCombinations
            {
                Hero = Convert.ToInt32(hero),
                LevelOne = level_one,
                LevelFour = level_four,
                LevelSeven = level_seven,
                LevelTen = level_ten,
                LevelThirteen = level_thirteen,
                LevelSixteen = level_sixteen,
                LevelTwenty = level_twenty
            };

            _context.TalentCombinations.Add(combo);
            _context.SaveChanges();

            return combo.TalentCombinationId;
        }

        private void UpdateGlobalTalentDataDetails(ReplayData data, MySqlConnection conn)
        {
            foreach (var player in data.Replay_Player)
            {
                var winLoss = player.Winner ? 1 : 0;

                if (player.Talents == null) continue;
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

                    var talentDetail = new GlobalHeroTalentsDetails
                    {
                            GameVersion = data.GameVersion,
                            GameType = Convert.ToSByte(data.GameType_id),
                            LeagueTier = Convert.ToSByte(player.player_league_tier),
                            HeroLeagueTier = Convert.ToSByte(player.hero_league_tier),
                            RoleLeagueTier = Convert.ToSByte(player.role_league_tier),
                            GameMap = Convert.ToSByte(data.GameMap_id),
                            HeroLevel = (uint) heroLevel,
                            Hero = Convert.ToSByte(player.Hero_id),
                            Mirror = (sbyte) player.Mirror,
                            // TODO: Region doesn't exist in the db?
                            // Region = data.Region
                            WinLoss = (sbyte) winLoss,
                            Level = Convert.ToInt32(level),
                            GameTime = (uint?) data.Length,
                            Kills = (uint?) player.Score.SoloKills,
                            Assists = (uint?) player.Score.Assists,
                            Takedowns = (uint?) player.Score.Takedowns,
                            Deaths = (uint?) player.Score.Deaths,
                            HighestKillStreak = (uint?) player.Score.HighestKillStreak,
                            HeroDamage = (uint?) player.Score.HeroDamage,
                            SiegeDamage = (uint?) player.Score.SiegeDamage,
                            StructureDamage = (uint?) player.Score.StructureDamage,
                            MinionDamage = (uint?) player.Score.MinionDamage,
                            CreepDamage = (uint?) player.Score.CreepDamage,
                            SummonDamage = (uint?) player.Score.SummonDamage,
                            TimeCcEnemyHeroes = (uint?) player.Score.TimeCCdEnemyHeroes,
                            Healing = (uint?) player.Score.Healing,
                            SelfHealing = (uint?) player.Score.SelfHealing,
                            DamageTaken = (uint?) player.Score.DamageTaken,
                            ExperienceContribution = (uint?) player.Score.ExperienceContribution,
                            TownKills = (uint?) player.Score.TownKills,
                            TimeSpentDead = (uint?) player.Score.TimeSpentDead,
                            MercCampCaptures = (uint?) player.Score.MercCampCaptures,
                            WatchTowerCaptures = (uint?) player.Score.WatchTowerCaptures,
                            ProtectionAllies = (uint?) player.Score.ProtectionGivenToAllies,
                            SilencingEnemies = (uint?) player.Score.TimeSilencingEnemyHeroes,
                            RootingEnemies = (uint?) player.Score.TimeRootingEnemyHeroes,
                            StunningEnemies = (uint?) player.Score.TimeStunningEnemyHeroes,
                            ClutchHeals = (uint?) player.Score.ClutchHealsPerformed,
                            Escapes = (uint?) player.Score.EscapesPerformed,
                            Vengeance = (uint?) player.Score.VengeancesPerformed,
                            OutnumberedDeaths = (uint?) player.Score.OutnumberedDeaths,
                            TeamfightEscapes = (uint?) player.Score.TeamfightEscapesPerformed,
                            TeamfightHealing = (uint?) player.Score.TeamfightHealingDone,
                            TeamfightDamageTaken = (uint?) player.Score.TeamfightDamageTaken,
                            TeamfightHeroDamage = (uint?) player.Score.TeamfightHeroDamage,
                            Multikill = (uint?) player.Score.Multikill,
                            PhysicalDamage = (uint?) player.Score.PhysicalDamage,
                            SpellDamage = (uint?) player.Score.SpellDamage,
                            RegenGlobes = (int?) player.Score.RegenGlobes,
                            GamesPlayed = 1
                    };

                    talentDetail.Talent = t switch
                    {
                            0 => player.Talents.Level_One,
                            1 => player.Talents.Level_Four,
                            2 => player.Talents.Level_Seven,
                            3 => player.Talents.Level_Ten,
                            4 => player.Talents.Level_Thirteen,
                            5 => player.Talents.Level_Sixteen,
                            6 => player.Talents.Level_Twenty,
                            _ => talentDetail.Talent
                    };

                    _context.GlobalHeroTalentsDetails.Upsert(talentDetail)
                            .WhenMatched(x => new GlobalHeroTalentsDetails
                            {
                                    GameTime = x.GameTime + talentDetail.GameTime,
                                    Kills = x.Kills + talentDetail.Kills,
                                    Assists = x.Assists + talentDetail.Assists,
                                    Takedowns = x.Takedowns + talentDetail.Takedowns,
                                    Deaths = x.Deaths + talentDetail.Deaths,
                                    HighestKillStreak = x.HighestKillStreak + talentDetail.HighestKillStreak,
                                    HeroDamage = x.HeroDamage + talentDetail.HeroDamage,
                                    SiegeDamage = x.SiegeDamage + talentDetail.SiegeDamage,
                                    StructureDamage = x.StructureDamage + talentDetail.StructureDamage,
                                    MinionDamage = x.MinionDamage + talentDetail.MinionDamage,
                                    CreepDamage = x.CreepDamage + talentDetail.CreepDamage,
                                    SummonDamage = x.SummonDamage + talentDetail.SummonDamage,
                                    TimeCcEnemyHeroes = x.TimeCcEnemyHeroes + talentDetail.TimeCcEnemyHeroes,
                                    Healing = x.Healing + talentDetail.Healing,
                                    SelfHealing = x.SelfHealing + talentDetail.SelfHealing,
                                    DamageTaken = x.DamageTaken + talentDetail.DamageTaken,
                                    ExperienceContribution = x.ExperienceContribution + talentDetail.ExperienceContribution,
                                    TownKills = x.TownKills + talentDetail.TownKills,
                                    TimeSpentDead = x.TimeSpentDead + talentDetail.TimeSpentDead,
                                    MercCampCaptures = x.MercCampCaptures + talentDetail.MercCampCaptures,
                                    WatchTowerCaptures = x.WatchTowerCaptures + talentDetail.WatchTowerCaptures,
                                    ProtectionAllies = x.ProtectionAllies + talentDetail.ProtectionAllies,
                                    SilencingEnemies = x.SilencingEnemies + talentDetail.SilencingEnemies,
                                    RootingEnemies = x.RootingEnemies + talentDetail.RootingEnemies,
                                    StunningEnemies = x.StunningEnemies + talentDetail.StunningEnemies,
                                    ClutchHeals = x.ClutchHeals + talentDetail.ClutchHeals,
                                    Escapes = x.Escapes + talentDetail.Escapes,
                                    Vengeance = x.Vengeance + talentDetail.Vengeance,
                                    OutnumberedDeaths = x.OutnumberedDeaths + talentDetail.OutnumberedDeaths,
                                    TeamfightEscapes = x.TeamfightEscapes + talentDetail.TeamfightEscapes,
                                    TeamfightHealing = x.TeamfightHealing + talentDetail.TeamfightHealing,
                                    TeamfightDamageTaken = x.TeamfightDamageTaken + talentDetail.TeamfightDamageTaken,
                                    TeamfightHeroDamage = x.TeamfightHeroDamage + talentDetail.TeamfightHeroDamage,
                                    Multikill = x.Multikill + talentDetail.Multikill,
                                    PhysicalDamage = x.PhysicalDamage + talentDetail.PhysicalDamage,
                                    SpellDamage = x.SpellDamage + talentDetail.SpellDamage,
                                    RegenGlobes = x.RegenGlobes + talentDetail.RegenGlobes,
                                    GamesPlayed = x.GamesPlayed + talentDetail.GamesPlayed,
                            }).Run();
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
