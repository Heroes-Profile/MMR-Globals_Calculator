using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class MasterMmrData
    {
        public int TypeValue { get; set; }
        public byte GameType { get; set; }
        public uint BlizzId { get; set; }
        public byte Region { get; set; }
        public double ConservativeRating { get; set; }
        public double Mean { get; set; }
        public double StandardDeviation { get; set; }
        public uint Win { get; set; }
        public uint Loss { get; set; }
    }
}
