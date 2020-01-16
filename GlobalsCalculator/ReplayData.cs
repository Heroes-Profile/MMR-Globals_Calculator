using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GlobalsCalculator
{
    public partial class ReplayData
    {
        public long Id { get; set; }
        public string GameType_id { get; set; }

        public DateTime GameDate { get; set; }

        public long Region { get; set; }

        public string GameVersion { get; set; }

        public int[][] Bans { get; set; }

        public string GameMap_id { get; set; }

        public long Length { get; set; }

        /*
        public string GameMap { get; set; }

        public long? GameLength { get; set; }

        public string GameVersion { get; set; }

        public Guid Fingerprint { get; set; }


        public bool Processed { get; set; }

        public bool Deleted { get; set; }

        public Uri Url { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public DateTimeOffset UpdatedAt { get; set; }

        */

        public ReplayPlayer[] Replay_Player { get; set; }

    }

    public partial class ReplayPlayer
    {
        //MMR Data
        public double player_conservative_rating { get; set; }
        public double player_mean { get; set; }
        public double player_standard_deviation { get; set; }

        public double role_conservative_rating { get; set; }
        public double role_mean { get; set; }
        public double role_standard_deviation { get; set; }

        public double hero_conservative_rating { get; set; }
        public double hero_mean { get; set; }
        public double hero_standard_deviation { get; set; }

        public string player_league_tier { get; set; }
        public string hero_league_tier { get; set; }
        public string role_league_tier { get; set; }

        public string Hero { get; set; }
        public string Role { get; set; }
        public string Hero_id { get; set; }

        public long BlizzId { get; set; }
        public bool Winner { get; set; }
        public long HeroLevel { get; set; }
        public long MasteryTaunt { get; set; }

        public Score Score { get; set; }

        public Talents Talents { get; set; }
        public int Mirror { get; set; }
        public long Team { get; set; }

        /*


        public string Role { get; set; }



        public bool Winner { get; set; }

        public string WinnerValue { get; set; }


        public long? Party { get; set; }

        //[JsonProperty("silenced")]
        //public bool? Silenced { get; set; }

        public string Battletag { get; set; }

        public string BattletagID { get; set; }

        public Dictionary<string, string> Talents { get; set; }

        public Dictionary<string, long?> Score { get; set; }
        */
    }

    public partial class Score
    {
        public long? SoloKills { get; set; }
        public long? Assists { get; set; }
        public long? Takedowns { get; set; }
        public long? Deaths { get; set; }

        public long? RegenGlobes { get; set; }

        public long? HighestKillStreak { get; set; }
        public long? HeroDamage { get; set; }
        public long? SiegeDamage { get; set; }
        public long? StructureDamage { get; set; }
        public long? MinionDamage { get; set; }
        public long? CreepDamage { get; set; }
        public long? SummonDamage { get; set; }
        public long? TimeCCdEnemyHeroes { get; set; }
        public long? Healing { get; set; }
        public long? SelfHealing { get; set; }
        public long? DamageTaken { get; set; }
        public long? ExperienceContribution { get; set; }
        public long? TownKills { get; set; }
        public long? TimeSpentDead { get; set; }
        public long? MercCampCaptures { get; set; }
        public long? WatchTowerCaptures { get; set; }
        public long? ProtectionGivenToAllies { get; set; }
        public long? TimeSilencingEnemyHeroes { get; set; }
        public long? TimeRootingEnemyHeroes { get; set; }
        public long? TimeStunningEnemyHeroes { get; set; }
        public long? ClutchHealsPerformed { get; set; }
        public long? EscapesPerformed { get; set; }
        public long? VengeancesPerformed { get; set; }
        public long? OutnumberedDeaths { get; set; }
        public long? TeamfightEscapesPerformed { get; set; }
        public long? TeamfightHealingDone { get; set; }
        public long? TeamfightDamageTaken { get; set; }
        public long? TeamfightHeroDamage { get; set; }
        public long? Multikill { get; set; }
        public long? PhysicalDamage { get; set; }
        public long? SpellDamage { get; set; }


        /*
        public long? Level { get; set; }

        public long RegenGlobes { get; set; }

        public long FirstToTen { get; set; }
        public long? DamageSoaked { get; set; }

        public long? MetaExperience { get; set; }





        public long[] MatchAwards { get; set; }
        */
    }

    public partial class Talents
    {
        public int Level_One { get; set; }
        public int Level_Four { get; set; }
        public int Level_Seven { get; set; }
        public int Level_Ten { get; set; }
        public int Level_Thirteen { get; set; }
        public int Level_Sixteen { get; set; }
        public int Level_Twenty { get; set; }
    }


}
