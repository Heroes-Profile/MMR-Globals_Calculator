using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class GlobalHeroStats
    {
        public string GameVersion { get; set; }
        public sbyte GameType { get; set; }
        public sbyte LeagueTier { get; set; }
        public sbyte HeroLeagueTier { get; set; }
        public sbyte RoleLeagueTier { get; set; }
        public sbyte GameMap { get; set; }
        public uint HeroLevel { get; set; }
        public sbyte Hero { get; set; }
        public sbyte Mirror { get; set; }
        public int Region { get; set; }
        public sbyte WinLoss { get; set; }
        public uint? GameTime { get; set; }
        public uint? Kills { get; set; }
        public uint? Assists { get; set; }
        public uint? Takedowns { get; set; }
        public uint? Deaths { get; set; }
        public uint? HighestKillStreak { get; set; }
        public uint? HeroDamage { get; set; }
        public uint? SiegeDamage { get; set; }
        public uint? StructureDamage { get; set; }
        public uint? MinionDamage { get; set; }
        public uint? CreepDamage { get; set; }
        public uint? SummonDamage { get; set; }
        public uint? TimeCcEnemyHeroes { get; set; }
        public uint? Healing { get; set; }
        public uint? SelfHealing { get; set; }
        public uint? DamageTaken { get; set; }
        public uint? ExperienceContribution { get; set; }
        public uint? TownKills { get; set; }
        public uint? TimeSpentDead { get; set; }
        public uint? MercCampCaptures { get; set; }
        public uint? WatchTowerCaptures { get; set; }
        public uint? ProtectionAllies { get; set; }
        public uint? SilencingEnemies { get; set; }
        public uint? RootingEnemies { get; set; }
        public uint? StunningEnemies { get; set; }
        public uint? ClutchHeals { get; set; }
        public uint? Escapes { get; set; }
        public uint? Vengeance { get; set; }
        public uint? OutnumberedDeaths { get; set; }
        public uint? TeamfightEscapes { get; set; }
        public uint? TeamfightHealing { get; set; }
        public uint? TeamfightDamageTaken { get; set; }
        public uint? TeamfightHeroDamage { get; set; }
        public uint? Multikill { get; set; }
        public uint? PhysicalDamage { get; set; }
        public uint? SpellDamage { get; set; }
        public int? RegenGlobes { get; set; }
        public uint GamesPlayed { get; set; }
    }
}
