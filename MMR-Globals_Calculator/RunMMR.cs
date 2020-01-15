using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MMR_Globals_Calculator
{
    class RunMMR
    {
        private string db_connect_string = new DB_Connect().heroesprofile_config;

        private Dictionary<string, string> mmr_ids = new Dictionary<string, string>();
        private Dictionary<string, string> role = new Dictionary<string, string>();
        private Dictionary<string, string> heroes = new Dictionary<string, string>();
        private Dictionary<string, string> heroes_ids = new Dictionary<string, string>();

        private Dictionary<int, int> replays_to_run = new Dictionary<int, int>();
        private Dictionary<string, string> players = new Dictionary<string, string>();

        private Dictionary<string, string> seasons_game_versions = new Dictionary<string, string>();

        public RunMMR()
        {
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT mmr_type_id, name FROM mmr_type_ids";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        mmr_ids.Add(Reader.GetString("name"), Reader.GetString("mmr_type_id"));
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT id, name, new_role FROM heroes";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        heroes.Add(Reader.GetString("name"), Reader.GetString("id"));
                        heroes_ids.Add(Reader.GetString("id"), Reader.GetString("name"));
                        role.Add(Reader.GetString("name"), Reader.GetString("new_role"));
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT season, game_version FROM season_game_versions";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        if (!seasons_game_versions.ContainsKey(Reader.GetString("game_version")))
                        {
                            seasons_game_versions.Add(Reader.GetString("game_version"), Reader.GetString("season"));

                        }
                    }
                }

                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT replay.replayID, replay.region, player.blizz_id FROM replay inner join player on player.replayID = replay.replayID where mmr_ran = 0 ORDER BY game_date ASC LIMIT 10000";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        if (!players.ContainsKey(Reader.GetString("blizz_id") + "|" + Reader.GetString("region")))
                        {
                            if (!replays_to_run.ContainsKey(Reader.GetInt32("replayID")))
                            {
                                replays_to_run.Add(Reader.GetInt32("replayID"), Reader.GetInt32("replayID"));

                            }
                            players.Add(Reader.GetString("blizz_id") + "|" + Reader.GetString("region"), Reader.GetString("blizz_id") + "|" + Reader.GetString("region"));
                        }
                    }
                }
            }
            Console.WriteLine("Finished  - Sleeping for 5 seconds before running");

            System.Threading.Thread.Sleep(5000);
            Parallel.ForEach(
                replays_to_run.Keys,
                //new ParallelOptions { MaxDegreeOfParallelism = -1 },
                new ParallelOptions { MaxDegreeOfParallelism = 1 },
                //new ParallelOptions { MaxDegreeOfParallelism = 10 },
                item =>
                {
                    Console.WriteLine("Running MMR data for replayID: " + item);
                    ReplayData data = new ReplayData();
                    data = getReplayData(item);
                    if (data.Replay_Player != null)
                    {

                        if (data.Replay_Player.Length == 10 && data.Replay_Player[9] != null)
                        {
                            data = calculateMMR(data);
                            updatePlayerMMR(data);
                            saveMasterMMRData(data);

                            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
                            {
                                conn.Open();
                                using (MySqlCommand cmd = conn.CreateCommand())
                                {
                                    cmd.CommandText = "UPDATE replay SET mmr_ran = 1 WHERE replayID = " + item;
                                    MySqlDataReader Reader = cmd.ExecuteReader();

                                }

                            }

                            if (Convert.ToInt32(seasons_game_versions[data.GameVersion]) >= 13)
                            {
                                using (MySqlConnection conn = new MySqlConnection(db_connect_string))
                                {
                                    conn.Open();
                                    updateGlobalHeroData(data, conn);
                                    updateGlobalTalentData(data, conn);
                                    updateGlobalTalentDataDetails(data, conn);
                                    updateMatchups(data, conn);
                                    updateDeathwingData(data, conn);
                                }


                            }

                        }

                    }



                });
        }

        private ReplayData getReplayData(int replayID)
        {
            ReplayData data = new ReplayData();
            Replay_Player[] players = new Replay_Player[10];
            int player_counter = 0;

            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                using (MySqlCommand cmd = conn.CreateCommand())
                {
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
                    "AND scores.battletag = player.battletag) WHERE replay.replayID = " + replayID + " order by team ASC";

                    //Console.WriteLine(cmd.CommandText);
                    MySqlDataReader Reader = cmd.ExecuteReader();
                    while (Reader.Read())
                    {
                        data.Id = Reader.GetInt64("replayID");
                        data.GameType_id = Reader.GetString("game_type");
                        data.Region = Reader.GetInt64("region");
                        data.GameDate = Reader.GetDateTime("game_date");
                        data.Length = Reader.GetInt64("game_length");

                        data.GameVersion = Reader.GetString("game_version");

                        data.Bans = getBans(data.Id);

                        data.GameMap_id = Reader.GetString("game_map");

                        Replay_Player player = new Replay_Player();
                        player.Hero_id = Reader.GetString("hero");
                        player.Hero = heroes_ids[Reader.GetString("hero")];
                        player.Role = role[player.Hero];
                        player.BlizzId = Reader.GetInt64("blizz_id");

                        int winner = Reader.GetInt32("winner");

                        if (winner == 1)
                        {
                            player.Winner = true;

                        }
                        else
                        {
                            player.Winner = false;

                        }
                        player.HeroLevel = Reader.GetInt64("hero_level");
                        player.MasteryTaunt = Reader.GetInt64("mastery_taunt");
                        player.Mirror = 0;
                        player.Team = Reader.GetInt32("team");

                        Talents talents = new Talents();
                        talents.Level_One = Reader.GetInt32("level_one");
                        talents.Level_Four = Reader.GetInt32("level_four");
                        talents.Level_Seven = Reader.GetInt32("level_seven");
                        talents.Level_Ten = Reader.GetInt32("level_ten");
                        talents.Level_Thirteen = Reader.GetInt32("level_thirteen");
                        talents.Level_Sixteen = Reader.GetInt32("level_Sixteen");
                        talents.Level_Twenty = Reader.GetInt32("level_Twenty");

                        player.Talents = talents;

                        Score score = new Score();

                        score.SoloKills = Reader.GetInt64("kills");
                        score.Assists = Reader.GetInt64("assists");
                        score.Takedowns = Reader.GetInt64("takedowns");
                        score.Deaths = Reader.GetInt64("deaths");
                        score.HighestKillStreak = Reader.GetInt64("highest_kill_streak");
                        score.HeroDamage = Reader.GetInt64("hero_damage");
                        score.SiegeDamage = Reader.GetInt64("siege_damage");
                        score.StructureDamage = Reader.GetInt64("structure_damage");
                        score.MinionDamage = Reader.GetInt64("minion_damage");
                        score.CreepDamage = Reader.GetInt64("creep_damage");
                        score.SummonDamage = Reader.GetInt64("summon_damage");
                        score.TimeCCdEnemyHeroes = Reader.GetInt64("time_cc_enemy_heroes");
                        score.Healing = Reader.GetInt64("healing");
                        score.SelfHealing = Reader.GetInt64("self_healing");
                        score.DamageTaken = Reader.GetInt64("damage_taken");
                        score.ExperienceContribution = Reader.GetInt64("experience_contribution");
                        score.TownKills = Reader.GetInt64("town_kills");
                        score.TimeSpentDead = Reader.GetInt64("time_spent_dead");
                        score.MercCampCaptures = Reader.GetInt64("merc_camp_captures");
                        score.WatchTowerCaptures = Reader.GetInt64("watch_tower_captures");
                        score.ProtectionGivenToAllies = Reader.GetInt64("protection_allies");
                        score.TimeSilencingEnemyHeroes = Reader.GetInt64("silencing_enemies");
                        score.TimeRootingEnemyHeroes = Reader.GetInt64("rooting_enemies");
                        score.TimeStunningEnemyHeroes = Reader.GetInt64("stunning_enemies");
                        score.ClutchHealsPerformed = Reader.GetInt64("clutch_heals");
                        score.EscapesPerformed = Reader.GetInt64("escapes");
                        score.VengeancesPerformed = Reader.GetInt64("vengeance");
                        score.OutnumberedDeaths = Reader.GetInt64("outnumbered_deaths");
                        score.TeamfightEscapesPerformed = Reader.GetInt64("teamfight_escapes");
                        score.TeamfightHealingDone = Reader.GetInt64("teamfight_healing");
                        score.TeamfightDamageTaken = Reader.GetInt64("teamfight_damage_taken");
                        score.TeamfightHeroDamage = Reader.GetInt64("teamfight_hero_damage");
                        score.Multikill = Reader.GetInt64("multikill");
                        score.PhysicalDamage = Reader.GetInt64("physical_damage");
                        score.SpellDamage = Reader.GetInt64("spell_damage");
                        score.RegenGlobes = Reader.GetInt64("regen_globes");


                        player.Score = score;

                        players[player_counter] = player;
                        player_counter++;

                    }
                }
            }


            if (players.Length == 10 && player_counter == 10)
            {
                if (players[9] != null)
                {
                    data.Replay_Player = players;

                    Replay_Player[] orderedPlayers = new Replay_Player[10];

                    int team1 = 0;
                    int team2 = 5;
                    for (int j = 0; j < data.Replay_Player.Length; j++)
                    {
                        if (data.Replay_Player[j].Team == 0)
                        {
                            orderedPlayers[team1] = data.Replay_Player[j];
                            team1++;
                        }
                        else if (data.Replay_Player[j].Team == 1)
                        {
                            orderedPlayers[team2] = data.Replay_Player[j];
                            team2++;
                        }
                    }
                    data.Replay_Player = orderedPlayers;

                    for (int i = 0; i < data.Replay_Player.Length; i++)
                    {
                        for (int j = 0; j < data.Replay_Player.Length; j++)
                        {
                            if (j != i)
                            {
                                if (data.Replay_Player[i].Hero == data.Replay_Player[j].Hero)
                                {
                                    data.Replay_Player[i].Mirror = 1;
                                    break;
                                }
                            }
                        }
                    }
                }

            }
            else
            {
                using (MySqlConnection conn = new MySqlConnection(db_connect_string))
                {
                    conn.Open();
                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "UPDATE replay SET mmr_ran = 1 WHERE replayID = " + replayID;
                        MySqlDataReader Reader = cmd.ExecuteReader();

                    }

                }
            }
            return data;
        }
        private int[][] getBans(long replayID)
        {
            int[][] bans = new int[2][];
            bans[0] = new int[3];
            bans[1] = new int[3];

            bans[0][0] = 0;
            bans[0][1] = 0;
            bans[0][2] = 0;

            bans[1][0] = 0;
            bans[1][1] = 0;
            bans[1][2] = 0;

            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * FROM heroesprofile.replay_bans where replayID = " + replayID;
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    int team_one_counter = 0;
                    int team_two_counter = 0;
                    while (Reader.Read())
                    {
                        if (Reader.GetInt32("team") == 0)
                        {
                            bans[0][team_one_counter] = Reader.GetInt32("hero");
                            team_one_counter++;
                        }
                        else
                        {
                            bans[1][team_two_counter] = Reader.GetInt32("hero");
                            team_two_counter++;
                        }
                    }
                }
            }



            return bans;
        }
        private ReplayData calculateMMR(ReplayData data)
        {

            MMRCalculator mmrCalcPlayer = new MMRCalculator(data, "player", mmr_ids, role);
            data = mmrCalcPlayer.data;
            MMRCalculator mmrCalcHero = new MMRCalculator(data, "hero", mmr_ids, role);
            data = mmrCalcPlayer.data;
            MMRCalculator mmrCalcRole = new MMRCalculator(data, "role", mmr_ids, role);
            data = mmrCalcPlayer.data;

            data = getLeagueTierData(data);

            return data;
        }

        private ReplayData getLeagueTierData(ReplayData data)
        {

            for (int i = 0; i < data.Replay_Player.Length; i++)
            {
                data.Replay_Player[i].player_league_tier = getLeague(mmr_ids["player"], data.GameType_id, (1800 + (data.Replay_Player[i].player_conservative_rating * 40)));
                data.Replay_Player[i].hero_league_tier = getLeague(data.Replay_Player[i].Hero_id, data.GameType_id, (1800 + (data.Replay_Player[i].hero_conservative_rating * 40)));
                data.Replay_Player[i].role_league_tier = getLeague(mmr_ids[data.Replay_Player[i].Role], data.GameType_id, (1800 + (data.Replay_Player[i].role_conservative_rating * 40)));
            }


            return data;
        }

        private string getLeague(string mmr_id, string game_type_id, double mmr)
        {
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();

                string league_tier = "";
                using (MySqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT league_tier FROM league_breakdowns where type_role_hero = " + mmr_id + " and game_type = " + game_type_id + " and min_mmr <= " + mmr + " order by min_mmr DESC LIMIT 1";
                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        league_tier = Reader.GetString("league_tier");

                    }

                    if (!Reader.HasRows)
                    {
                        league_tier = "1";
                    }
                }

                return league_tier;
            }

        }

        private void updatePlayerMMR(ReplayData data)
        {
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                for (int i = 0; i < data.Replay_Player.Length; i++)
                {
                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "UPDATE player SET " +
                            "player_conservative_rating = " + data.Replay_Player[i].player_conservative_rating + ", " +
                            "player_mean = " + data.Replay_Player[i].player_mean + ", " +
                            "player_standard_deviation = " + data.Replay_Player[i].player_standard_deviation + ", " +
                            "hero_conservative_rating = " + data.Replay_Player[i].hero_conservative_rating + ", " +
                            "hero_mean = " + data.Replay_Player[i].hero_mean + ", " +
                            "hero_standard_deviation = " + data.Replay_Player[i].hero_standard_deviation + ", " +
                            "role_conservative_rating = " + data.Replay_Player[i].role_conservative_rating + ", " +
                            "role_mean = " + data.Replay_Player[i].role_mean + ", " +
                            "role_standard_deviation = " + data.Replay_Player[i].role_standard_deviation + ", " +
                            "mmr_date_parsed = " + "\"" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "\"" +
                            " WHERE replayID = " + data.Id +
                            " AND blizz_id = " + data.Replay_Player[i].BlizzId;

                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }
                }
            }
        }

        private void saveMasterMMRData(ReplayData data)
        {
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();

                for (int i = 0; i < data.Replay_Player.Length; i++)
                {
                    int win = 0;
                    int loss = 0;

                    if (data.Replay_Player[i].Winner)
                    {
                        win = 1;
                        loss = 0;
                    }
                    else
                    {
                        win = 0;
                        loss = 1;
                    }
                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO master_mmr_data (type_value, game_type, blizz_id, region, conservative_rating, mean, standard_deviation, win, loss) VALUES(" +
                            "\"" + mmr_ids["player"] + "\"" + "," +
                            data.GameType_id + "," +
                            data.Replay_Player[i].BlizzId + "," +
                            data.Region + "," +
                            data.Replay_Player[i].player_conservative_rating + "," +
                            data.Replay_Player[i].player_mean + "," +
                            data.Replay_Player[i].player_standard_deviation + "," +
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
                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }



                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO master_mmr_data (type_value, game_type, blizz_id, region, conservative_rating, mean, standard_deviation, win, loss) VALUES(" +
                            "\"" + mmr_ids[role[data.Replay_Player[i].Hero]] + "\"" + "," +
                            data.GameType_id + "," +
                            data.Replay_Player[i].BlizzId + "," +
                            data.Region + "," +
                            data.Replay_Player[i].role_conservative_rating + "," +
                            data.Replay_Player[i].role_mean + "," +
                            data.Replay_Player[i].role_standard_deviation + "," +
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
                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }

                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO master_mmr_data (type_value, game_type, blizz_id, region, conservative_rating, mean, standard_deviation, win, loss) VALUES(" +
                            "\"" + data.Replay_Player[i].Hero_id + "\"" + "," +
                            data.GameType_id + "," +
                            data.Replay_Player[i].BlizzId + "," +
                            data.Region + "," +
                            data.Replay_Player[i].hero_conservative_rating + "," +
                            data.Replay_Player[i].hero_mean + "," +
                            data.Replay_Player[i].hero_standard_deviation + "," +
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
                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }
                }

            }
        }

        private void updateGlobalHeroData(ReplayData data, MySqlConnection conn)
        {

            for (int i = 0; i < data.Replay_Player.Length; i++)
            {

                int win_loss = 0;
                if (data.Replay_Player[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }




                if (data.Replay_Player[i].Score != null)
                {
                    int hero_level = 0;

                    if (data.Replay_Player[i].HeroLevel < 5)
                    {
                        hero_level = 1;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 5 && data.Replay_Player[i].HeroLevel < 10)
                    {
                        hero_level = 5;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 10 && data.Replay_Player[i].HeroLevel < 15)
                    {
                        hero_level = 10;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 15 && data.Replay_Player[i].HeroLevel < 20)
                    {
                        hero_level = 15;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 20)
                    {
                        if (data.Replay_Player[i].MasteryTaunt == 0)
                        {
                            hero_level = 20;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 1)
                        {
                            hero_level = 25;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 2)
                        {
                            hero_level = 40;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 3)
                        {
                            hero_level = 60;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 4)
                        {
                            hero_level = 80;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 5)
                        {
                            hero_level = 100;
                        }
                    }

                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
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
                        "\"" + data.Replay_Player[i].player_league_tier + "\"" + "," +
                        "\"" + data.Replay_Player[i].hero_league_tier + "\"" + "," +
                        "\"" + data.Replay_Player[i].role_league_tier + "\"" + "," +
                        "\"" + data.GameMap_id + "\"" + "," +
                        "\"" + hero_level + "\"" + "," +
                        "\"" + data.Replay_Player[i].Hero_id + "\"" + "," +
                        "\"" + data.Replay_Player[i].Mirror + "\"" + "," +
                        "\"" + data.Region + "\"" + "," +
                        "\"" + win_loss + "\"" + "," +
                        "\"" + data.Length + "\"" + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.SoloKills) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.Assists) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.Takedowns) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.Deaths) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.HighestKillStreak) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.HeroDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.SiegeDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.StructureDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.MinionDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.CreepDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.SummonDamage) + "," +
                        checkIfEmpty(Convert.ToInt64(data.Replay_Player[i].Score.TimeCCdEnemyHeroes)) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.Healing) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.SelfHealing) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.DamageTaken) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.ExperienceContribution) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TownKills) + "," +
                        checkIfEmpty(Convert.ToInt64(data.Replay_Player[i].Score.TimeSpentDead)) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.MercCampCaptures) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.WatchTowerCaptures) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.ProtectionGivenToAllies) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TimeSilencingEnemyHeroes) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TimeRootingEnemyHeroes) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TimeStunningEnemyHeroes) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.ClutchHealsPerformed) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.EscapesPerformed) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.VengeancesPerformed) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.OutnumberedDeaths) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightEscapesPerformed) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightHealingDone) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightDamageTaken) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightHeroDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.Multikill) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.PhysicalDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.SpellDamage) + "," +
                        checkIfEmpty(data.Replay_Player[i].Score.RegenGlobes) + "," +
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
                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }
                }
            }

            double team_one_avg_conservative_rating = 0;
            double team_one_avg_hero_conservative_rating = 0;
            double team_one_avg_role_conservative_rating = 0;

            double team_two_avg_conservative_rating = 0;
            double team_two_avg_hero_conservative_rating = 0;
            double team_two_avg_role_conservative_rating = 0;

            for (int i = 0; i < data.Replay_Player.Length; i++)
            {
                if (data.Replay_Player[i].Team == 0)
                {
                    team_one_avg_conservative_rating += Convert.ToDouble(data.Replay_Player[i].player_league_tier);
                    team_one_avg_hero_conservative_rating += Convert.ToDouble(data.Replay_Player[i].hero_league_tier);
                    team_one_avg_role_conservative_rating += Convert.ToDouble(data.Replay_Player[i].role_league_tier);
                }
                else
                {
                    team_two_avg_conservative_rating += Convert.ToDouble(data.Replay_Player[i].player_league_tier);
                    team_two_avg_hero_conservative_rating += Convert.ToDouble(data.Replay_Player[i].hero_league_tier);
                    team_two_avg_role_conservative_rating += Convert.ToDouble(data.Replay_Player[i].role_league_tier);

                }
            }


            team_one_avg_conservative_rating /= 5;
            team_one_avg_hero_conservative_rating /= 5;
            team_one_avg_role_conservative_rating /= 5;

            if (Math.Truncate(team_one_avg_conservative_rating) >= .5)
            {
                team_one_avg_conservative_rating = Math.Ceiling(team_one_avg_conservative_rating);
            }
            else
            {
                team_one_avg_conservative_rating = Math.Floor(team_one_avg_conservative_rating);
            }

            if (Math.Truncate(team_one_avg_hero_conservative_rating) >= .5)
            {
                team_one_avg_hero_conservative_rating = Math.Ceiling(team_one_avg_hero_conservative_rating);
            }
            else
            {
                team_one_avg_hero_conservative_rating = Math.Floor(team_one_avg_hero_conservative_rating);
            }

            if (Math.Truncate(team_one_avg_role_conservative_rating) >= .5)
            {
                team_one_avg_role_conservative_rating = Math.Ceiling(team_one_avg_role_conservative_rating);
            }
            else
            {
                team_one_avg_role_conservative_rating = Math.Floor(team_one_avg_role_conservative_rating);
            }


            team_two_avg_conservative_rating /= 5;
            team_two_avg_hero_conservative_rating /= 5;
            team_two_avg_role_conservative_rating /= 5;

            if (Math.Truncate(team_two_avg_conservative_rating) > .5)
            {
                team_two_avg_conservative_rating = Math.Ceiling(team_two_avg_conservative_rating);
            }
            else
            {
                team_two_avg_conservative_rating = Math.Floor(team_two_avg_conservative_rating);
            }

            if (Math.Truncate(team_two_avg_hero_conservative_rating) > .5)
            {
                team_two_avg_hero_conservative_rating = Math.Ceiling(team_two_avg_hero_conservative_rating);
            }
            else
            {
                team_two_avg_hero_conservative_rating = Math.Floor(team_two_avg_hero_conservative_rating);
            }

            if (Math.Truncate(team_two_avg_role_conservative_rating) > .5)
            {
                team_two_avg_role_conservative_rating = Math.Ceiling(team_two_avg_role_conservative_rating);
            }
            else
            {
                team_two_avg_role_conservative_rating = Math.Floor(team_two_avg_role_conservative_rating);
            }



            double team_one_avg_hero_level = 0;
            double team_two_avg_hero_level = 0;

            for (int i = 0; i < data.Replay_Player.Length; i++)
            {
                int hero_level = 0;

                if (data.Replay_Player[i].HeroLevel < 5)
                {
                    hero_level = 1;
                }
                else if (data.Replay_Player[i].HeroLevel >= 5 && data.Replay_Player[i].HeroLevel < 10)
                {
                    hero_level = 5;
                }
                else if (data.Replay_Player[i].HeroLevel >= 10 && data.Replay_Player[i].HeroLevel < 15)
                {
                    hero_level = 10;
                }
                else if (data.Replay_Player[i].HeroLevel >= 15 && data.Replay_Player[i].HeroLevel < 20)
                {
                    hero_level = 15;
                }
                else if (data.Replay_Player[i].HeroLevel >= 20)
                {
                    if (data.Replay_Player[i].MasteryTaunt == 0)
                    {
                        hero_level = 20;
                    }
                    else if (data.Replay_Player[i].MasteryTaunt == 1)
                    {
                        hero_level = 25;
                    }
                    else if (data.Replay_Player[i].MasteryTaunt == 2)
                    {
                        hero_level = 40;
                    }
                    else if (data.Replay_Player[i].MasteryTaunt == 3)
                    {
                        hero_level = 60;
                    }
                    else if (data.Replay_Player[i].MasteryTaunt == 4)
                    {
                        hero_level = 80;
                    }
                    else if (data.Replay_Player[i].MasteryTaunt == 5)
                    {
                        hero_level = 100;
                    }
                }

                if (data.Replay_Player[i].Team == 0)
                {
                    team_one_avg_hero_level += hero_level;

                }
                else
                {
                    team_two_avg_hero_level += hero_level;

                }
            }





            team_one_avg_hero_level /= 5;
            team_two_avg_hero_level /= 5;

            if (team_one_avg_hero_level < 5)
            {
                team_one_avg_hero_level = 1;
            }
            else if (team_one_avg_hero_level >= 5 && team_one_avg_hero_level < 10)
            {
                team_one_avg_hero_level = 5;
            }
            else if (team_one_avg_hero_level >= 10 && team_one_avg_hero_level < 15)
            {
                team_one_avg_hero_level = 10;
            }
            else if (team_one_avg_hero_level >= 15 && team_one_avg_hero_level < 20)
            {
                team_one_avg_hero_level = 15;
            }
            else if (team_one_avg_hero_level >= 20 && team_one_avg_hero_level < 25)
            {
                team_one_avg_hero_level = 20;
            }
            else if (team_one_avg_hero_level >= 25 && team_one_avg_hero_level < 40)
            {
                team_one_avg_hero_level = 25;
            }
            else if (team_one_avg_hero_level >= 40 && team_one_avg_hero_level < 60)
            {
                team_one_avg_hero_level = 40;
            }
            else if (team_one_avg_hero_level >= 60 && team_one_avg_hero_level < 80)
            {
                team_one_avg_hero_level = 60;
            }
            else if (team_one_avg_hero_level >= 80 && team_one_avg_hero_level < 100)
            {
                team_one_avg_hero_level = 80;
            }
            else if (team_one_avg_hero_level >= 100)
            {
                team_one_avg_hero_level = 100;
            }


            if (team_two_avg_hero_level < 5)
            {
                team_two_avg_hero_level = 1;
            }
            else if (team_two_avg_hero_level >= 5 && team_two_avg_hero_level < 10)
            {
                team_two_avg_hero_level = 5;
            }
            else if (team_two_avg_hero_level >= 10 && team_two_avg_hero_level < 15)
            {
                team_two_avg_hero_level = 10;
            }
            else if (team_two_avg_hero_level >= 15 && team_two_avg_hero_level < 20)
            {
                team_two_avg_hero_level = 15;
            }
            else if (team_two_avg_hero_level >= 20 && team_two_avg_hero_level < 25)
            {
                team_two_avg_hero_level = 20;
            }
            else if (team_two_avg_hero_level >= 25 && team_two_avg_hero_level < 40)
            {
                team_two_avg_hero_level = 25;
            }
            else if (team_two_avg_hero_level >= 40 && team_two_avg_hero_level < 60)
            {
                team_two_avg_hero_level = 40;
            }
            else if (team_two_avg_hero_level >= 60 && team_two_avg_hero_level < 80)
            {
                team_two_avg_hero_level = 60;
            }
            else if (team_two_avg_hero_level >= 80 && team_two_avg_hero_level < 100)
            {
                team_two_avg_hero_level = 80;
            }
            else if (team_two_avg_hero_level >= 100)
            {
                team_two_avg_hero_level = 100;
            }




            if (data.Bans != null)
            {
                for (int i = 0; i < data.Bans.Length; i++)
                {
                    for (int j = 0; j < data.Bans[i].Length; j++)
                    {

                        int value = data.Bans[i][j];


                        using (MySqlCommand cmd = conn.CreateCommand())
                        {
                            cmd.CommandText = "INSERT INTO global_hero_stats_bans (game_version, game_type, league_tier, hero_league_tier, role_league_tier, game_map, hero_level, region, hero, bans) VALUES (" +
                                "\"" + data.GameVersion + "\"" + "," +
                                "\"" + data.GameType_id + "\"" + ",";

                            if (i == 0)
                            {
                                cmd.CommandText += "\"" + team_one_avg_conservative_rating + "\"" + ",";
                                cmd.CommandText += "\"" + team_one_avg_hero_conservative_rating + "\"" + ",";
                                cmd.CommandText += "\"" + team_one_avg_role_conservative_rating + "\"" + ",";
                            }
                            else
                            {
                                cmd.CommandText += "\"" + team_two_avg_conservative_rating + "\"" + ",";
                                cmd.CommandText += "\"" + team_two_avg_hero_conservative_rating + "\"" + ",";
                                cmd.CommandText += "\"" + team_two_avg_role_conservative_rating + "\"" + ",";

                            }
                            cmd.CommandText += "\"" + data.GameMap_id + "\"" + ",";

                            if (i == 0)
                            {
                                cmd.CommandText += "\"" + team_one_avg_hero_level + "\"" + ",";
                            }
                            else
                            {
                                cmd.CommandText += "\"" + team_two_avg_hero_level + "\"" + ",";

                            }
                            cmd.CommandText += data.Region + ",";
                            cmd.CommandText += "\"" + value + "\"" + "," +
                                "\"" + 1 + "\"" + ")";
                            cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                "bans = bans + VALUES(bans)";
                            //Console.WriteLine(cmd.CommandText);
                            MySqlDataReader Reader = cmd.ExecuteReader();
                        }


                    }
                }
            }


        }

        private void updateMatchups(ReplayData data, MySqlConnection conn)
        {
            //


            for (int i = 0; i < data.Replay_Player.Length; i++)
            {

                int win_loss = 0;
                if (data.Replay_Player[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }




                if (data.Replay_Player[i].Score != null)
                {
                    int hero_level = 0;

                    if (data.Replay_Player[i].HeroLevel < 5)
                    {
                        hero_level = 1;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 5 && data.Replay_Player[i].HeroLevel < 10)
                    {
                        hero_level = 5;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 10 && data.Replay_Player[i].HeroLevel < 15)
                    {
                        hero_level = 10;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 15 && data.Replay_Player[i].HeroLevel < 20)
                    {
                        hero_level = 15;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 20)
                    {
                        if (data.Replay_Player[i].MasteryTaunt == 0)
                        {
                            hero_level = 20;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 1)
                        {
                            hero_level = 25;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 2)
                        {
                            hero_level = 40;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 3)
                        {
                            hero_level = 60;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 4)
                        {
                            hero_level = 80;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 5)
                        {
                            hero_level = 100;
                        }
                    }

                    for (int j = 0; j < data.Replay_Player.Length; j++)
                    {
                        if (data.Replay_Player[j].BlizzId != data.Replay_Player[i].BlizzId)
                        {
                            using (MySqlCommand cmd = conn.CreateCommand())
                            {
                                if (data.Replay_Player[j].Team == data.Replay_Player[i].Team)
                                {
                                    cmd.CommandText = "INSERT INTO global_hero_matchups_ally (game_version, game_type, league_tier, hero_league_tier, role_league_tier, game_map, hero_level, hero, ally, mirror, region, win_loss, games_played) VALUES (";

                                }
                                else
                                {
                                    cmd.CommandText = "INSERT INTO global_hero_matchups_enemy (game_version, game_type, league_tier, hero_league_tier, role_league_tier, game_map, hero_level, hero, enemy, mirror, region, win_loss, games_played) VALUES (";
                                }
                                cmd.CommandText += "\"" + data.GameVersion + "\"" + "," +
                                    "\"" + data.GameType_id + "\"" + "," +
                                    "\"" + data.Replay_Player[i].player_league_tier + "\"" + "," +
                                    "\"" + data.Replay_Player[i].hero_league_tier + "\"" + "," +
                                    "\"" + data.Replay_Player[i].role_league_tier + "\"" + "," +
                                    "\"" + data.GameMap_id + "\"" + "," +
                                     "\"" + hero_level + "\"" + "," +
                                    "\"" + data.Replay_Player[i].Hero_id + "\"" + "," +
                                    "\"" + data.Replay_Player[j].Hero_id + "\"" + "," +
                                    "\"" + data.Replay_Player[i].Mirror + "\"" + "," +
                                    "\"" + data.Region + "\"" + "," +
                                    "\"" + win_loss + "\"" + "," +
                                    1 + ")";


                                cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                                    "games_played = games_played + VALUES(games_played)";
                                //Console.WriteLine(cmd.CommandText);
                                MySqlDataReader Reader = cmd.ExecuteReader();
                            }

                        }


                    }
                }
            }

        }
        private void updateGlobalTalentData(ReplayData data, MySqlConnection conn)
        {
            for (int i = 0; i < data.Replay_Player.Length; i++)
            {

                int win_loss = 0;
                if (data.Replay_Player[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }

                if (data.Replay_Player[i].Score != null)
                {

                    int hero_level = 0;

                    if (data.Replay_Player[i].HeroLevel < 5)
                    {
                        hero_level = 1;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 5 && data.Replay_Player[i].HeroLevel < 10)
                    {
                        hero_level = 5;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 10 && data.Replay_Player[i].HeroLevel < 15)
                    {
                        hero_level = 10;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 15 && data.Replay_Player[i].HeroLevel < 20)
                    {
                        hero_level = 15;
                    }
                    else if (data.Replay_Player[i].HeroLevel >= 20)
                    {
                        if (data.Replay_Player[i].MasteryTaunt == 0)
                        {
                            hero_level = 20;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 1)
                        {
                            hero_level = 25;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 2)
                        {
                            hero_level = 40;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 3)
                        {
                            hero_level = 60;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 4)
                        {
                            hero_level = 80;
                        }
                        else if (data.Replay_Player[i].MasteryTaunt == 5)
                        {
                            hero_level = 100;
                        }
                    }

                    using (MySqlCommand cmd = conn.CreateCommand())
                    {

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
                        "\"" + data.Replay_Player[i].player_league_tier + "\"" + "," +
                        "\"" + data.Replay_Player[i].hero_league_tier + "\"" + "," +
                        "\"" + data.Replay_Player[i].role_league_tier + "\"" + "," +
                        "\"" + data.GameMap_id + "\"" + "," +
                        "\"" + hero_level + "\"" + "," +
                        "\"" + data.Replay_Player[i].Hero_id + "\"" + "," +
                        "\"" + data.Replay_Player[i].Mirror + "\"" + "," +
                        "\"" + data.Region + "\"" + "," +
                        "\"" + win_loss + "\"" + ",";



                        if (data.Replay_Player[i].Talents == null || data.Replay_Player[i].Talents.Level_One == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0) + ",";
                        }
                        else if (data.Replay_Player[i].Talents.Level_Four == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0) + ",";

                        }
                        else if (data.Replay_Player[i].Talents.Level_Seven == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                data.Replay_Player[i].Talents.Level_Four,
                                0,
                                0,
                                0,
                                0,
                                0) + ",";
                        }
                        else if (data.Replay_Player[i].Talents.Level_Ten == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                data.Replay_Player[i].Talents.Level_Four,
                                data.Replay_Player[i].Talents.Level_Seven,
                                0,
                                0,
                                0,
                                0) + ",";
                        }
                        else if (data.Replay_Player[i].Talents.Level_Thirteen == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                data.Replay_Player[i].Talents.Level_Four,
                                data.Replay_Player[i].Talents.Level_Seven,
                                data.Replay_Player[i].Talents.Level_Ten,
                                0,
                                0,
                                0) + ",";

                        }
                        else if (data.Replay_Player[i].Talents.Level_Sixteen == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                data.Replay_Player[i].Talents.Level_Four,
                                data.Replay_Player[i].Talents.Level_Seven,
                                data.Replay_Player[i].Talents.Level_Ten,
                                data.Replay_Player[i].Talents.Level_Thirteen,
                                0,
                                0) + ",";
                        }
                        else if (data.Replay_Player[i].Talents.Level_Twenty == 0)
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                data.Replay_Player[i].Talents.Level_Four,
                                data.Replay_Player[i].Talents.Level_Seven,
                                data.Replay_Player[i].Talents.Level_Ten,
                                data.Replay_Player[i].Talents.Level_Thirteen,
                                data.Replay_Player[i].Talents.Level_Sixteen,
                                0) + ",";
                        }
                        else
                        {
                            cmd.CommandText += getHeroCombID(
                                data.Replay_Player[i].Hero_id,
                                data.Replay_Player[i].Talents.Level_One,
                                data.Replay_Player[i].Talents.Level_Four,
                                data.Replay_Player[i].Talents.Level_Seven,
                                data.Replay_Player[i].Talents.Level_Ten,
                                data.Replay_Player[i].Talents.Level_Thirteen,
                                data.Replay_Player[i].Talents.Level_Sixteen,
                                data.Replay_Player[i].Talents.Level_Twenty) + ",";
                        }

                        cmd.CommandText += "\"" + data.Length + "\"" + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SoloKills) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Assists) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Takedowns) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Deaths) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.HighestKillStreak) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.HeroDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SiegeDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.StructureDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.MinionDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.CreepDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SummonDamage) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Replay_Player[i].Score.TimeCCdEnemyHeroes)) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Healing) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SelfHealing) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.DamageTaken) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.ExperienceContribution) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TownKills) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Replay_Player[i].Score.TimeSpentDead)) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.MercCampCaptures) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.WatchTowerCaptures) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.ProtectionGivenToAllies) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TimeSilencingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TimeRootingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TimeStunningEnemyHeroes) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.ClutchHealsPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.EscapesPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.VengeancesPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.OutnumberedDeaths) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightEscapesPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightHealingDone) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightDamageTaken) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightHeroDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Multikill) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.PhysicalDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SpellDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.RegenGlobes) + "," +
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
                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }
                }


            }

        }

        private int getHeroCombID(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            int comb_id = 0;
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                using (MySqlCommand cmd = conn.CreateCommand())
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

                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        comb_id = Reader.GetInt32("talent_combination_id");
                    }
                    if (!Reader.HasRows)
                    {
                        comb_id = insertTalentCombo(hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty);
                    }
                }
            }

            return comb_id;
        }
        private int insertTalentCombo(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            int comb_id = 0;

            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
                conn.Open();
                using (MySqlCommand cmd = conn.CreateCommand())
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

                    MySqlDataReader Reader = cmd.ExecuteReader();
                }


                using (MySqlCommand cmd = conn.CreateCommand())
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

                    MySqlDataReader Reader = cmd.ExecuteReader();

                    while (Reader.Read())
                    {
                        comb_id = Reader.GetInt32("talent_combination_id");
                    }
                }
            }

            return comb_id;
        }
        private void updateGlobalTalentDataDetails(ReplayData data, MySqlConnection conn)
        {
            for (int i = 0; i < data.Replay_Player.Length; i++)
            {

                int win_loss = 0;
                if (data.Replay_Player[i].Winner)
                {

                    win_loss = 1;
                }
                else
                {

                    win_loss = 0;
                }

                if (data.Replay_Player[i].Talents != null)
                {
                    for (int t = 0; t < 7; t++)
                    {
                        string level = "";
                        if (t == 0)
                        {
                            level = "1";
                        }
                        else if (t == 1)
                        {
                            level = "4";

                        }
                        else if (t == 2)
                        {
                            level = "7";

                        }
                        else if (t == 3)
                        {
                            level = "10";

                        }
                        else if (t == 4)
                        {
                            level = "13";

                        }
                        else if (t == 5)
                        {
                            level = "16";

                        }
                        else if (t == 6)
                        {
                            level = "20";

                        }

                        if (data.Replay_Player[i].Score != null)
                        {
                            int hero_level = 0;

                            if (data.Replay_Player[i].HeroLevel < 5)
                            {
                                hero_level = 1;
                            }
                            else if (data.Replay_Player[i].HeroLevel >= 5 && data.Replay_Player[i].HeroLevel < 10)
                            {
                                hero_level = 5;
                            }
                            else if (data.Replay_Player[i].HeroLevel >= 10 && data.Replay_Player[i].HeroLevel < 15)
                            {
                                hero_level = 10;
                            }
                            else if (data.Replay_Player[i].HeroLevel >= 15 && data.Replay_Player[i].HeroLevel < 20)
                            {
                                hero_level = 15;
                            }
                            else if (data.Replay_Player[i].HeroLevel >= 20)
                            {
                                if (data.Replay_Player[i].MasteryTaunt == 0)
                                {
                                    hero_level = 20;
                                }
                                else if (data.Replay_Player[i].MasteryTaunt == 1)
                                {
                                    hero_level = 25;
                                }
                                else if (data.Replay_Player[i].MasteryTaunt == 2)
                                {
                                    hero_level = 40;
                                }
                                else if (data.Replay_Player[i].MasteryTaunt == 3)
                                {
                                    hero_level = 60;
                                }
                                else if (data.Replay_Player[i].MasteryTaunt == 4)
                                {
                                    hero_level = 80;
                                }
                                else if (data.Replay_Player[i].MasteryTaunt == 5)
                                {
                                    hero_level = 100;
                                }
                            }

                            using (MySqlCommand cmd = conn.CreateCommand())
                            {

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
                                    data.Replay_Player[i].player_league_tier + "," +
                                    data.Replay_Player[i].hero_league_tier + "," +
                                    data.Replay_Player[i].role_league_tier + "," +
                                    data.GameMap_id + "," +
                                    hero_level + "," +
                                    data.Replay_Player[i].Hero_id + "," +
                                    data.Replay_Player[i].Mirror + "," +
                                    data.Region + "," +
                                    "\"" + win_loss + "\"" + "," +
                                    level + ",";

                                if (t == 0)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_One + ",";
                                }
                                else if (t == 1)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_Four + ",";
                                }
                                else if (t == 2)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_Seven + ",";
                                }
                                else if (t == 3)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_Ten + ",";
                                }
                                else if (t == 4)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_Thirteen + ",";
                                }
                                else if (t == 5)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_Sixteen + ",";
                                }
                                else if (t == 6)
                                {
                                    cmd.CommandText += data.Replay_Player[i].Talents.Level_Twenty + ",";
                                }


                                cmd.CommandText += "\"" + data.Length + "\"" + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SoloKills) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Assists) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Takedowns) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Deaths) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.HighestKillStreak) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.HeroDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SiegeDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.StructureDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.MinionDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.CreepDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SummonDamage) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Replay_Player[i].Score.TimeCCdEnemyHeroes)) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Healing) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SelfHealing) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.DamageTaken) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.ExperienceContribution) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TownKills) + "," +
                                        checkIfEmpty(Convert.ToInt64(data.Replay_Player[i].Score.TimeSpentDead)) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.MercCampCaptures) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.WatchTowerCaptures) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.ProtectionGivenToAllies) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TimeSilencingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TimeRootingEnemyHeroes) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TimeStunningEnemyHeroes) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.ClutchHealsPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.EscapesPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.VengeancesPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.OutnumberedDeaths) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightEscapesPerformed) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightHealingDone) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightDamageTaken) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.TeamfightHeroDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.Multikill) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.PhysicalDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.SpellDamage) + "," +
                                        checkIfEmpty(data.Replay_Player[i].Score.RegenGlobes) + "," +
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
                                MySqlDataReader Reader = cmd.ExecuteReader();
                            }
                        }
                    }
                }

            }

        }

        private void updateDeathwingData(ReplayData data, MySqlConnection conn)
        {

            for (int i = 0; i < data.Replay_Player.Length; i++)
            {
                int buildings_destroyed = 0;
                int villagers_slain = 0;
                int raiders_killed = 0;
                int deathwing_killed = 0;
                if (data.Replay_Player[i].Hero_id == "89")
                {
                    buildings_destroyed += Convert.ToInt32(data.Replay_Player[i].Score.SiegeDamage);
                    raiders_killed += Convert.ToInt32(data.Replay_Player[i].Score.Takedowns);
                    villagers_slain += (Convert.ToInt32(data.Replay_Player[i].Score.CreepDamage) + Convert.ToInt32(data.Replay_Player[i].Score.MinionDamage) + Convert.ToInt32(data.Replay_Player[i].Score.SummonDamage));
                    deathwing_killed += Convert.ToInt32(data.Replay_Player[i].Score.Deaths);



                    villagers_slain /= 812;
                    buildings_destroyed /= 12900;

                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO deathwing_data (game_type, buildings_destroyed, villagers_slain, raiders_killed, deathwing_killed) VALUES(" +
                            data.GameType_id + "," +
                            buildings_destroyed + "," +
                            villagers_slain + "," +
                            raiders_killed + "," +
                            deathwing_killed + ")";
                        cmd.CommandText += " ON DUPLICATE KEY UPDATE " +
                            "buildings_destroyed = buildings_destroyed + VALUES(buildings_destroyed)," +
                            "villagers_slain = villagers_slain + VALUES(villagers_slain)," +
                            "raiders_killed = raiders_killed + VALUES(raiders_killed)," +
                            "deathwing_killed = deathwing_killed + VALUES(deathwing_killed)";

                        cmd.CommandTimeout = 0;
                        //Console.WriteLine(cmd.CommandText);
                        MySqlDataReader Reader = cmd.ExecuteReader();
                    }
                }

            }
        }

        private string checkIfEmpty(long? value)
        {
            if (value == null)
            {
                return "NULL";
            }
            else
            {
                return value.ToString();
            }
        }
    }
}
