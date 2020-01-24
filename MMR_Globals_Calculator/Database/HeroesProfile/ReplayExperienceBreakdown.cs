using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class ReplayExperienceBreakdown
    {
        public int ReplayId { get; set; }
        public sbyte Team { get; set; }
        public int TeamLevel { get; set; }
        public string Timestamp { get; set; }
        public double? StructureXp { get; set; }
        public double? CreepXp { get; set; }
        public double? HeroXp { get; set; }
        public double? MinionXp { get; set; }
        public double? TrickXp { get; set; }
        public double? TotalXp { get; set; }
    }
}
