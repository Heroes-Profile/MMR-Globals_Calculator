using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileCache
{
    public partial class GlobalHeroChange
    {
        public string GameVersion { get; set; }
        public sbyte GameType { get; set; }
        public sbyte Hero { get; set; }
        public double WinRate { get; set; }
        public double Popularity { get; set; }
        public double BanRate { get; set; }
        public int GamesPlayed { get; set; }
        public int Wins { get; set; }
        public int Losses { get; set; }
        public int Bans { get; set; }
    }
}
