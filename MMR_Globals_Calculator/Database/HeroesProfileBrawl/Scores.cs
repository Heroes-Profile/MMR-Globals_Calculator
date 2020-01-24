using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileBrawl
{
    public partial class Scores
    {
        public int ReplayId { get; set; }
        public string Battletag { get; set; }
        public int Level { get; set; }
        public int Kills { get; set; }
        public int Assists { get; set; }
        public int Takedowns { get; set; }
        public int Deaths { get; set; }
        public int HighestKillStreak { get; set; }
        public int HeroDamage { get; set; }
        public int SiegeDamage { get; set; }
        public int StructureDamage { get; set; }
        public int MinionDamage { get; set; }
        public int CreepDamage { get; set; }
        public int SummonDamage { get; set; }
        public int TimeCcEnemyHeroes { get; set; }
        public int Healing { get; set; }
        public int SelfHealing { get; set; }
        public int DamageTaken { get; set; }
        public int ExperienceContribution { get; set; }
        public int TownKills { get; set; }
        public int TimeSpentDead { get; set; }
        public int MercCampCaptures { get; set; }
        public int WatchTowerCaptures { get; set; }
        public int MetaExperience { get; set; }
        public int MatchAward { get; set; }
        public int ProtectionAllies { get; set; }
        public int SilencingEnemies { get; set; }
        public int RootingEnemies { get; set; }
        public int StunningEnemies { get; set; }
        public int ClutchHeals { get; set; }
        public int Escapes { get; set; }
        public int Vengeance { get; set; }
        public int OutnumberedDeaths { get; set; }
        public int TeamfightEscapes { get; set; }
        public int TeamfightHealing { get; set; }
        public int TeamfightDamageTaken { get; set; }
        public int TeamfightHeroDamage { get; set; }
        public int Multikill { get; set; }
        public int PhysicalDamage { get; set; }
        public int SpellDamage { get; set; }
        public int RegenGlobes { get; set; }
        public int FirstToTen { get; set; }
    }
}
