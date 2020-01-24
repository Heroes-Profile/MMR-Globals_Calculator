using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class MasterGamesPlayedData
    {
        public int TypeValue { get; set; }
        public double Season { get; set; }
        public byte GameType { get; set; }
        public uint BlizzId { get; set; }
        public byte Region { get; set; }
        public int? Win { get; set; }
        public int? Loss { get; set; }
        public int? GamesPlayed { get; set; }
    }
}
