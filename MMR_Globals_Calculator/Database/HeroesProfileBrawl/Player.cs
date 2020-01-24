using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileBrawl
{
    public partial class Player
    {
        public int ReplayId { get; set; }
        public int BlizzId { get; set; }
        public string Battletag { get; set; }
        public sbyte Hero { get; set; }
        public short HeroLevel { get; set; }
        public short MasteryTaunt { get; set; }
        public sbyte Team { get; set; }
        public sbyte Winner { get; set; }
        public string Party { get; set; }
    }
}
