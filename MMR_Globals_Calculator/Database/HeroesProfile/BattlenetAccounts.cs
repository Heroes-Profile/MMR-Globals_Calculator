using System;
using System.Collections.Generic;

namespace MMR_Globals_Calculator.Database.HeroesProfile
{
    public partial class BattlenetAccounts
    {
        public ulong Id { get; set; }
        public int BattlenetId { get; set; }
        public string Battletag { get; set; }
        public int Region { get; set; }
        public string BattlenetAccessToken { get; set; }
        public string RememberToken { get; set; }
        public DateTime? CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
    }
}
