using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfileCache
{
    public partial class Leaderboard
    {
        public int GameType { get; set; }
        public int Season { get; set; }
        public int Type { get; set; }
        public int Rank { get; set; }
        public string SplitBattletag { get; set; }
        public string Battletag { get; set; }
        public int BlizzId { get; set; }
        public sbyte Region { get; set; }
        public double WinRate { get; set; }
        public int Win { get; set; }
        public int Loss { get; set; }
        public int GamesPlayed { get; set; }
        public double ConservativeRating { get; set; }
        public double Rating { get; set; }
        public int CacheNumber { get; set; }
    }
}
