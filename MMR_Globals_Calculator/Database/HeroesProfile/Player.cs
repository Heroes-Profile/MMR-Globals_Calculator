using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class Player
    {
        public uint ReplayId { get; set; }
        public uint BlizzId { get; set; }
        public string Battletag { get; set; }
        public byte Hero { get; set; }
        public ushort HeroLevel { get; set; }
        public ushort? MasteryTaunt { get; set; }
        public byte Team { get; set; }
        public byte Winner { get; set; }
        public string Party { get; set; }
        public double? PlayerConservativeRating { get; set; }
        public double? PlayerMean { get; set; }
        public double? PlayerStandardDeviation { get; set; }
        public double? HeroConservativeRating { get; set; }
        public double? HeroMean { get; set; }
        public double? HeroStandardDeviation { get; set; }
        public double? RoleConservativeRating { get; set; }
        public double? RoleMean { get; set; }
        public double? RoleStandardDeviation { get; set; }
        public DateTime? MmrDateParsed { get; set; }
    }
}
