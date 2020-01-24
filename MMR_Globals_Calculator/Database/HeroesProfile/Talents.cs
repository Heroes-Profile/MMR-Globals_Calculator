using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class Talents
    {
        public uint ReplayId { get; set; }
        public int Battletag { get; set; }
        public int? LevelOne { get; set; }
        public int? LevelFour { get; set; }
        public int? LevelSeven { get; set; }
        public int? LevelTen { get; set; }
        public int? LevelThirteen { get; set; }
        public int? LevelSixteen { get; set; }
        public int? LevelTwenty { get; set; }
    }
}
