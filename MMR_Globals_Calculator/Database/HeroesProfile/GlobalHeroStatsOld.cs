using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class GlobalHeroStatsOld
    {
        public string GameVersion { get; set; }
        public sbyte GameType { get; set; }
        public sbyte Hero { get; set; }
        public sbyte WinLoss { get; set; }
        public uint GamesPlayed { get; set; }
    }
}
