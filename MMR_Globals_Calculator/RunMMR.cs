using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using MMR_Globals_Calculator.Database.HeroesProfile;
using MMR_Globals_Calculator.Helpers;
using MySql.Data.MySqlClient;
using Z.EntityFramework.Plus;

namespace MMR_Globals_Calculator
{
    public class RunMmrResult
    {
        public Dictionary<string, string> Roles = new Dictionary<string, string>();
        public Dictionary<string, string> Heroes = new Dictionary<string, string>();
        public Dictionary<string, string> HeroesIds = new Dictionary<string, string>();
        public List<int> ReplaysToRun = new List<int>();
        public Dictionary<string, string> Players = new Dictionary<string, string>();
        public Dictionary<string, string> SeasonsGameVersions = new Dictionary<string, string>();
    }
    internal class RunMmrService
    {
        private readonly HeroesProfileContext _context;
        private readonly MmrCalculatorService _mmrCalculatorService;

        public RunMmrService(HeroesProfileContext context, MmrCalculatorService mmrCalculatorService)
        {
            _context = context;
            _mmrCalculatorService = mmrCalculatorService;
        }


        public async Task<RunMmrResult> RunMmr()
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

            foreach (var version in seasonGameVersions.Where(version =>
                    !result.SeasonsGameVersions.ContainsKey(version.GameVersion)))
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

            foreach (var replay in replays.Where(replay =>
                    !result.Players.ContainsKey(replay.BlizzId + "|" + replay.Region)))
            {
                if (!result.ReplaysToRun.Contains((int) replay.ReplayId))
                { 
                    result.ReplaysToRun.Add((int) replay.ReplayId);
                }

                result.Players.Add(replay.BlizzId + "|" + replay.Region, replay.BlizzId + "|" + replay.Region);
            }

            Console.WriteLine("Finished  - Sleeping for 5 seconds before running");
            System.Threading.Thread.Sleep(5000);

            //TODO: Leaving Degrees Of Parallelism at 1 here b/c that's how it was before. Can we increase this? It makes it complete wayyyy faster
            await result.ReplaysToRun.ForEachAsync(1, async replayId =>
            {
                Console.WriteLine("Running MMR data for replayID: " + replayId);
                var data = await GetReplayData(replayId, result.Roles, result.HeroesIds);
                if (data.ReplayPlayer == null) return;
                if (data.ReplayPlayer.Length != 10 || data.ReplayPlayer[9] == null) return;
                data = await CalculateMmr(data, mmrTypeIds, result.Roles);
                await UpdatePlayerMmr(data);
                await SaveMasterMmrData(data, mmrTypeIds.ToDictionary(x => x.Name, x => x.MmrTypeId), result.Roles);

                await _context.Replay
                              .Where(x => x.ReplayId == replayId)
                              .UpdateAsync(x => new Replay {MmrRan = 1});
                await _context.SaveChangesAsync();

                if (result.SeasonsGameVersions.ContainsKey(data.GameVersion) &&
                    Convert.ToInt32(result.SeasonsGameVersions[data.GameVersion]) < 13) return;
                {
                    await UpdateGlobalHeroData(data);
                    await UpdateGlobalTalentData(data);
                    await UpdateGlobalTalentDataDetails(data);
                    await UpdateMatchups(data);
                    await UpdateDeathwingData(data);
                }
            });

            return result;
        }

        private async Task<ReplayData> GetReplayData(int replayId, Dictionary<string, string> roles,
                                                     Dictionary<string, string> heroIds)
        {
            var data = new ReplayData();
            var players = new ReplayPlayer[10];
            var playerCounter = 0;

            var replay = await _context.Replay.FirstOrDefaultAsync(x => x.ReplayId == replayId);

            if (replay == null) return data;
            {
                //Replay level items
                data.Id = replay.ReplayId;
                data.GameTypeId = replay.GameType.ToString();
                data.Region = replay.Region;
                data.GameDate = replay.GameDate;
                data.Length = replay.GameLength;
                data.GameVersion = replay.GameVersion;
                data.Bans = await GetBans(data.Id);
                data.GameMapId = replay.GameMap.ToString();

                //Player level items
                var replayPlayers = await _context.Player.Where(x => x.ReplayId == replayId).OrderBy(x => x.Team)
                                                  .ToListAsync();
                foreach (var player in replayPlayers)
                {
                    //Queries
                    var replayTalent = await _context.Talents.FirstOrDefaultAsync(x => x.ReplayId == replayId
                                                                                    && x.Battletag.ToString() ==
                                                                                       player.Battletag);
                    var score = await _context.Scores
                                              .FirstOrDefaultAsync(x => x.ReplayId == replayId
                                                                     && x.Battletag == player.Battletag);

                    var heroId = player.Hero.ToString();
                    var hero = heroIds[player.Hero.ToString()];
                    var role = roles[hero];

                    //Assignments
                    var replayPlayer = new ReplayPlayer
                    {
                            HeroId = heroId,
                            Hero = hero,
                            Role = role,
                            BlizzId = player.BlizzId,
                            Winner = player.Winner == 1,
                            HeroLevel = player.HeroLevel,
                            MasteryTaunt = player.MasteryTaunt ?? 0,
                            Mirror = 0,
                            Team = player.Team,
                            Talents = new Talents
                            {
                                    LevelOne = replayTalent?.LevelOne ?? 0,
                                    LevelFour = replayTalent?.LevelFour ?? 0,
                                    LevelSeven = replayTalent?.LevelSeven ?? 0,
                                    LevelTen = replayTalent?.LevelTen ?? 0,
                                    LevelThirteen = replayTalent?.LevelThirteen ?? 0,
                                    LevelSixteen = replayTalent?.LevelSixteen ?? 0,
                                    LevelTwenty = replayTalent?.LevelTwenty ?? 0
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
                    data.ReplayPlayer = players;

                    var orderedPlayers = new ReplayPlayer[10];

                    var team1 = 0;
                    var team2 = 5;
                    foreach (var replayPlayer in data.ReplayPlayer)
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

                    data.ReplayPlayer = orderedPlayers;

                    for (var i = 0; i < data.ReplayPlayer.Length; i++)
                    {
                        for (var j = 0; j < data.ReplayPlayer.Length; j++)
                        {
                            if (j == i) continue;
                            if (data.ReplayPlayer[i].Hero != data.ReplayPlayer[j].Hero) continue;
                            data.ReplayPlayer[i].Mirror = 1;
                            break;
                        }
                    }
                }
                else
                {
                    await _context.Replay
                                  .Where(x => x.ReplayId == replayId)
                                  .UpdateAsync(x => new Replay
                                  {
                                          MmrRan = 1
                                  });
                }
            }

            return data;
        }

        private async Task<int[][]> GetBans(long replayId)
        {
            var bans = new int[2][];
            bans[0] = new int[3];
            bans[1] = new int[3];

            var teamOneCounter = 0;
            var teamTwoCounter = 0;

            var replayBans = await _context.ReplayBans.Where(x => x.ReplayId == replayId).ToListAsync();

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

        private async Task<ReplayData> CalculateMmr(ReplayData data, IEnumerable<MmrTypeIds> mmrTypeIds, Dictionary<string, string> roles)
        {
            var mmrTypeIdsDict = mmrTypeIds.ToDictionary(x => x.Name, x => x.MmrTypeId);

            var mmrCalcPlayer = await _mmrCalculatorService.TwoPlayerTestNotDrawn(data, "player", mmrTypeIdsDict, roles);
            data = mmrCalcPlayer;
            var mmrCalcHero = await _mmrCalculatorService.TwoPlayerTestNotDrawn(data, "hero", mmrTypeIdsDict, roles);

            //TODO: Should this actually be assigning from mmrCalcHero?
            data = mmrCalcPlayer;
            var mmrCalcRole = await _mmrCalculatorService.TwoPlayerTestNotDrawn(data, "role", mmrTypeIdsDict, roles);

            //TODO: Should this actually be assigning from mmrCalcRole?
            data = mmrCalcPlayer;

            data = await GetLeagueTierData(data, mmrTypeIdsDict);

            return data;
        }

        private async Task<ReplayData> GetLeagueTierData(ReplayData data, Dictionary<string, uint> mmrTypeIdsDict)
        {
            foreach (var r in data.ReplayPlayer)
            {
                r.PlayerLeagueTier = await GetLeague(mmrTypeIdsDict["player"], data.GameTypeId, (1800 + (r.PlayerConservativeRating * 40)));
                r.HeroLeagueTier = await GetLeague(Convert.ToUInt32(r.HeroId), data.GameTypeId, (1800 + (r.HeroConservativeRating * 40)));
                r.RoleLeagueTier = await GetLeague(mmrTypeIdsDict[r.Role], data.GameTypeId, (1800 + (r.RoleConservativeRating * 40)));
            }
            return data;
        }

        private async Task<string> GetLeague(uint mmrId, string gameTypeId, double mmr)
        {
            var leagueBreakdown = await _context.LeagueBreakdowns.Where(x => x.TypeRoleHero == mmrId
                                                                       && x.GameType == Convert.ToSByte(gameTypeId)
                                                                       && x.MinMmr <= mmr)
                .OrderByDescending(x => x.MinMmr)
                .Take(1)
                .FirstOrDefaultAsync();

            return leagueBreakdown == null ? "1" : leagueBreakdown.LeagueTier.ToString();
        }

        private async Task UpdatePlayerMmr(ReplayData data)
        {
            foreach (var r in data.ReplayPlayer)
            {
                await _context.Player
                    .Where(x => x.ReplayId == data.Id
                                && x.BlizzId == r.BlizzId)
                    .UpdateAsync(x => new Player
                    {
                        PlayerConservativeRating = r.PlayerConservativeRating,
                        PlayerMean = r.PlayerMean,
                        PlayerStandardDeviation = r.PlayerStandardDeviation,
                        HeroConservativeRating = r.HeroConservativeRating,
                        HeroMean = r.HeroMean,
                        HeroStandardDeviation = r.HeroStandardDeviation,
                        RoleConservativeRating = r.RoleConservativeRating,
                        RoleMean = r.RoleMean,
                        RoleStandardDeviation = r.RoleStandardDeviation,
                        MmrDateParsed = DateTime.Now
                    });
            }

            await _context.SaveChangesAsync();
        }

        private async Task SaveMasterMmrData(ReplayData data, Dictionary<string, uint> mmrTypeIdsDict,
            Dictionary<string, string> roles)
        {
            foreach (var r in data.ReplayPlayer)
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
                    GameType = Convert.ToByte(data.GameTypeId),
                    BlizzId = (uint) r.BlizzId,
                    Region = (byte) data.Region,
                    ConservativeRating = r.PlayerConservativeRating,
                    Mean = r.PlayerMean,
                    StandardDeviation = r.PlayerStandardDeviation,
                    Win = win,
                    Loss = loss
                };

                await _context.MasterMmrData.Upsert(masterMmrDataPlayer)
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
                    }).RunAsync();

                var masterMmrDataRole = new MasterMmrData
                {
                    TypeValue = (int) mmrTypeIdsDict[roles[r.Hero]],
                    GameType = Convert.ToByte(data.GameTypeId),
                    BlizzId = (uint) r.BlizzId,
                    Region = (byte) data.Region,
                    ConservativeRating = r.RoleConservativeRating,
                    Mean = r.RoleMean,
                    StandardDeviation = r.RoleStandardDeviation,
                    Win = win,
                    Loss = loss
                };

                await _context.MasterMmrData.Upsert(masterMmrDataRole)
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
                    }).RunAsync();

                var masterMmrDataHero = new MasterMmrData
                {
                    TypeValue = Convert.ToInt32(r.HeroId),
                    GameType = Convert.ToByte(data.GameTypeId),
                    BlizzId = (uint) r.BlizzId,
                    Region = (byte) data.Region,
                    ConservativeRating = r.HeroConservativeRating,
                    Mean = r.HeroMean,
                    StandardDeviation = r.HeroStandardDeviation,
                    Win = win,
                    Loss = loss
                };

                await _context.MasterMmrData.Upsert(masterMmrDataHero)
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
                    }).RunAsync();

            }
        }

        private async Task UpdateGlobalHeroData(ReplayData data)
        {
            foreach (var player in data.ReplayPlayer)
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
                        GameType = Convert.ToSByte(data.GameTypeId),
                        LeagueTier = Convert.ToSByte(player.PlayerLeagueTier),
                        HeroLeagueTier = Convert.ToSByte(player.HeroLeagueTier),
                        RoleLeagueTier = Convert.ToSByte(player.RoleLeagueTier),
                        GameMap = Convert.ToSByte(data.GameMapId),
                        HeroLevel = (uint) heroLevel,
                        Hero = Convert.ToSByte(player.HeroId),
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

                await _context.GlobalHeroStats.Upsert(globalHeroStats)
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
                                      TeamfightDamageTaken =
                                              x.TeamfightDamageTaken + globalHeroStats.TeamfightDamageTaken,
                                      TeamfightHeroDamage =
                                              x.TeamfightHeroDamage + globalHeroStats.TeamfightHeroDamage,
                                      Multikill = x.Multikill + globalHeroStats.Multikill,
                                      PhysicalDamage = x.PhysicalDamage + globalHeroStats.PhysicalDamage,
                                      SpellDamage = x.SpellDamage + globalHeroStats.SpellDamage,
                                      RegenGlobes = x.RegenGlobes + globalHeroStats.RegenGlobes,
                                      GamesPlayed = x.GamesPlayed + globalHeroStats.GamesPlayed,
                              }).RunAsync();
            }

            double teamOneAvgConservativeRating = 0;
            double teamOneAvgHeroConservativeRating = 0;
            double teamOneAvgRoleConservativeRating = 0;

            double teamTwoAvgConservativeRating = 0;
            double teamTwoAvgHeroConservativeRating = 0;
            double teamTwoAvgRoleConservativeRating = 0;

            foreach (var r in data.ReplayPlayer)
            {
                if (r.Team == 0)
                {
                    teamOneAvgConservativeRating += Convert.ToDouble(r.PlayerLeagueTier);
                    teamOneAvgHeroConservativeRating += Convert.ToDouble(r.HeroLeagueTier);
                    teamOneAvgRoleConservativeRating += Convert.ToDouble(r.RoleLeagueTier);
                }
                else
                {
                    teamTwoAvgConservativeRating += Convert.ToDouble(r.PlayerLeagueTier);
                    teamTwoAvgHeroConservativeRating += Convert.ToDouble(r.HeroLeagueTier);
                    teamTwoAvgRoleConservativeRating += Convert.ToDouble(r.RoleLeagueTier);

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

            foreach (var r in data.ReplayPlayer)
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
                                GameType = Convert.ToSByte(data.GameTypeId),
                                LeagueTier =
                                        (sbyte) (i == 0 ? teamOneAvgConservativeRating : teamTwoAvgConservativeRating),
                                HeroLeagueTier = (sbyte) (i == 0
                                        ? teamOneAvgHeroConservativeRating
                                        : teamTwoAvgHeroConservativeRating),
                                RoleLeagueTier = (sbyte) (i == 0
                                        ? teamOneAvgRoleConservativeRating
                                        : teamTwoAvgRoleConservativeRating),
                                GameMap = Convert.ToSByte(data.GameMapId),
                                HeroLevel = (sbyte) (i == 0 ? teamOneAvgHeroLevel : teamTwoAvgHeroLevel),
                                // TODO : Region doesn't exist in the db?
                                // Region = data.Region
                                Hero = (sbyte) value,
                                Bans = 1
                        };
                        await _context.GlobalHeroStatsBans.Upsert(globalHeroStatsBans)
                                      .WhenMatched(x => new GlobalHeroStatsBans
                                      {
                                              Bans = x.Bans + globalHeroStatsBans.Bans
                                      }).RunAsync();
                    }
                }
            }
        }

        private async Task UpdateMatchups(ReplayData data)
        {
            foreach (var r in data.ReplayPlayer)
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

                foreach (var t in data.ReplayPlayer)
                {
                    if (t.BlizzId == r.BlizzId) continue;

                    if (t.Team == r.Team)
                    {
                        var matchup = new GlobalHeroMatchupsAlly
                        {
                                GameVersion = data.GameVersion,
                                GameType = Convert.ToSByte(data.GameTypeId),
                                LeagueTier = Convert.ToSByte(r.PlayerLeagueTier),
                                HeroLeagueTier = Convert.ToSByte(r.HeroLeagueTier),
                                RoleLeagueTier = Convert.ToSByte(r.RoleLeagueTier),
                                GameMap = Convert.ToSByte(data.GameMapId),
                                HeroLevel = (uint) heroLevel,
                                Hero = Convert.ToSByte(r.HeroId),
                                Ally = Convert.ToSByte(t.HeroId),
                                Mirror = (sbyte) r.Mirror,
                                // TODO: Region column doesn't exist in seed data?
                                // Region = data.Region
                                WinLoss = (sbyte) winLoss,
                                GamesPlayed = 1
                        };
                        await _context.GlobalHeroMatchupsAlly.Upsert(matchup)
                                      .WhenMatched(x => new GlobalHeroMatchupsAlly
                                      {
                                              GamesPlayed = x.GamesPlayed + matchup.GamesPlayed
                                      }).RunAsync();
                    }
                    else
                    {
                        var matchup = new GlobalHeroMatchupsEnemy
                        {
                                GameVersion = data.GameVersion,
                                GameType = Convert.ToSByte(data.GameTypeId),
                                LeagueTier = Convert.ToSByte(r.PlayerLeagueTier),
                                HeroLeagueTier = Convert.ToSByte(r.HeroLeagueTier),
                                RoleLeagueTier = Convert.ToSByte(r.RoleLeagueTier),
                                GameMap = Convert.ToSByte(data.GameMapId),
                                HeroLevel = (uint) heroLevel,
                                Hero = Convert.ToSByte(r.HeroId),
                                Enemy = Convert.ToSByte(t.HeroId),
                                Mirror = (sbyte) r.Mirror,
                                // TODO: Region column doesn't exist in seed data?
                                // Region = data.Region
                                WinLoss = (sbyte) winLoss,
                                GamesPlayed = 1
                        };
                        await _context.GlobalHeroMatchupsEnemy.Upsert(matchup)
                                      .WhenMatched(x => new GlobalHeroMatchupsEnemy
                                      {
                                              GamesPlayed = x.GamesPlayed + matchup.GamesPlayed
                                      }).RunAsync();
                    }
                }
            }
        }

        private async Task UpdateGlobalTalentData(ReplayData data)
        {
            foreach (var player in data.ReplayPlayer)
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
                    talentComboId = await GetOrInsertHeroTalentComboId(player.HeroId, 0, 0, 0, 0, 0, 0, 0);
                }
                else
                {
                    talentComboId = await GetOrInsertHeroTalentComboId(player.HeroId,
                            player.Talents.LevelOne,
                            player.Talents.LevelFour,
                            player.Talents.LevelSeven,
                            player.Talents.LevelTen,
                            player.Talents.LevelThirteen,
                            player.Talents.LevelSixteen,
                            player.Talents.LevelTwenty);
                }

                var talent = new GlobalHeroTalents
                {
                        GameVersion = data.GameVersion,
                        GameType = Convert.ToSByte(data.GameTypeId),
                        LeagueTier = Convert.ToSByte(player.PlayerLeagueTier),
                        HeroLeagueTier = Convert.ToSByte(player.HeroLeagueTier),
                        RoleLeagueTier = Convert.ToSByte(player.RoleLeagueTier),
                        GameMap = Convert.ToSByte(data.GameMapId),
                        HeroLevel = (uint) heroLevel,
                        Hero = Convert.ToSByte(player.HeroId),
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

                await _context.GlobalHeroTalents.Upsert(talent)
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
                                      ExperienceContribution =
                                              x.ExperienceContribution + talent.ExperienceContribution,
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
                              }).RunAsync();
            }
        }

        private async Task<int> GetOrInsertHeroTalentComboId(string hero, int level_one, int level_four, int level_seven, int level_ten, int level_thirteen, int level_sixteen, int level_twenty)
        {
            var talentCombo = await _context.TalentCombinations.FirstOrDefaultAsync(x =>
                x.Hero == Convert.ToInt32(hero)
                && x.LevelOne == level_one
                && x.LevelFour == level_four
                && x.LevelSeven == level_seven
                && x.LevelTen == level_ten
                && x.LevelThirteen == level_thirteen
                && x.LevelSixteen == level_sixteen
                && x.LevelTwenty == level_twenty);

            var combId = talentCombo?.TalentCombinationId ?? await InsertTalentCombo(hero, level_one, level_four, level_seven, level_ten, level_thirteen, level_sixteen, level_twenty);

            return combId;
        }

        private async Task<int> InsertTalentCombo(string hero, int level_one, int level_four, int level_seven, int level_ten,
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

            await _context.TalentCombinations.AddAsync(combo);
            await _context.SaveChangesAsync();

            return combo.TalentCombinationId;
        }

        private async Task UpdateGlobalTalentDataDetails(ReplayData data)
        {
            foreach (var player in data.ReplayPlayer)
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
                            GameType = Convert.ToSByte(data.GameTypeId),
                            LeagueTier = Convert.ToSByte(player.PlayerLeagueTier),
                            HeroLeagueTier = Convert.ToSByte(player.HeroLeagueTier),
                            RoleLeagueTier = Convert.ToSByte(player.RoleLeagueTier),
                            GameMap = Convert.ToSByte(data.GameMapId),
                            HeroLevel = (uint) heroLevel,
                            Hero = Convert.ToSByte(player.HeroId),
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
                            0 => player.Talents.LevelOne,
                            1 => player.Talents.LevelFour,
                            2 => player.Talents.LevelSeven,
                            3 => player.Talents.LevelTen,
                            4 => player.Talents.LevelThirteen,
                            5 => player.Talents.LevelSixteen,
                            6 => player.Talents.LevelTwenty,
                            _ => talentDetail.Talent
                    };

                    await _context.GlobalHeroTalentsDetails.Upsert(talentDetail)
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
                            }).RunAsync();
                }
            }
        }

        private async Task UpdateDeathwingData(ReplayData data)
        {
            //TODO: The deathwing_data table doesn't exist in the seeded dbs?

            foreach (var player in data.ReplayPlayer)
            {
                if (player.HeroId != "89") continue;

                var buildingsDestroyed = 0;
                var villagersSlain = 0;
                var raidersKilled = 0;
                var deathwingKilled = 0;

                buildingsDestroyed += Convert.ToInt32(player.Score.SiegeDamage);
                raidersKilled += Convert.ToInt32(player.Score.Takedowns);
                villagersSlain += (Convert.ToInt32(player.Score.CreepDamage) +
                                   Convert.ToInt32(player.Score.MinionDamage) +
                                   Convert.ToInt32(player.Score.SummonDamage));
                deathwingKilled += Convert.ToInt32(player.Score.Deaths);

                villagersSlain /= 812;
                buildingsDestroyed /= 12900;

                var commandText =
                        "INSERT INTO deathwing_data (game_type, buildings_destroyed, villagers_slain, raiders_killed, deathwing_killed) VALUES(" +
                        data.GameTypeId + "," +
                        buildingsDestroyed + "," +
                        villagersSlain + "," +
                        raidersKilled + "," +
                        deathwingKilled + ")";
                commandText += " ON DUPLICATE KEY UPDATE " +
                               "buildings_destroyed = buildings_destroyed + VALUES(buildings_destroyed)," +
                               "villagers_slain = villagers_slain + VALUES(villagers_slain)," +
                               "raiders_killed = raiders_killed + VALUES(raiders_killed)," +
                               "deathwing_killed = deathwing_killed + VALUES(deathwing_killed)";
                var command = Helpers.DbHelpers.RawSqlQuery(
                        _context, commandText, x => new { });
                var a = 1;
            }
        }
    }
}
