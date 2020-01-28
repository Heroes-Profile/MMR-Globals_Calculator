using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using HeroesProfileDb.HeroesProfile;
using Moserware.Skills;
using Player = Moserware.Skills.Player;

namespace MMR_Globals_Calculator
{
    public class MmrCalculatorService
    {
        private readonly HeroesProfileContext _context;
        private const double ErrorTolerance = 0.085;
        private uint _typeId;

        private bool _teamOneWinner = false;
        private bool _teamTwoWinner = false;
        private double[] _playerMmRs = new double[10];
        private double[] _playerConserv = new double[10];

        public MmrCalculatorService(HeroesProfileContext context)
        {
            _context = context;
        }

        public async Task<ReplayData> TwoPlayerTestNotDrawn(ReplayData data, string type, Dictionary<string, uint> mmrIds, Dictionary<string, string> role)
        {
            // The algorithm has several parameters that can be tweaked that are
            // found in the "GameInfo" class. If you're just starting out, simply
            // use the defaults:
            var gameInfo = GameInfo.DefaultGameInfo;


            // Here's the most simple case: you have two players and one wins 
            // against the other.

            // Let's new up two players. Note that the argument passed into to Player
            // can be anything. This allows you to wrap any object. Here I'm just 
            // using a simple integer to represent the player, but you could just as
            // easily pass in a database entity representing a person/user or any
            var playerRatings = new Rating[10];
            for (var i = 0; i < 10; i++)
            {
                _playerMmRs[i] = 0;
                _playerConserv[i] = 0;
                playerRatings[i] = gameInfo.DefaultRating;
            }

            for (var i = 0; i < 10; i++)
            {
                _typeId = type switch
                {
                        "player" => mmrIds["player"],
                        "hero"   => mmrIds[data.ReplayPlayer[i].Hero],
                        "role"   => mmrIds[role[data.ReplayPlayer[i].Hero]],
                        _        => _typeId
                };

                var masterMmrData = await _context.MasterMmrData.Where(x => x.TypeValue == _typeId
                                                                         && x.GameType.ToString() == data.GameTypeId
                                                                         && x.BlizzId == data.ReplayPlayer[i].BlizzId
                                                                         && x.Region == data.Region).ToListAsync();
                var count = 0;
                foreach (var mmrData in masterMmrData)
                {
                    _playerConserv[count] = mmrData.ConservativeRating;
                    playerRatings[count] = new Rating(mmrData.Mean, mmrData.StandardDeviation);
                    count++;
                }
            }

            _teamOneWinner = data.ReplayPlayer[0].Winner;
            _teamTwoWinner = data.ReplayPlayer[9].Winner;

            var players = new Player[10];

            for (var i = 0; i < 10; i++)
            {
                players[i] = new Player(i);
            }

            var team1 = new Team()
                    .AddPlayer(players[0], playerRatings[0])
                    .AddPlayer(players[1], playerRatings[1])
                    .AddPlayer(players[2], playerRatings[2])
                    .AddPlayer(players[3], playerRatings[3])
                    .AddPlayer(players[4], playerRatings[4]);

            var team2 = new Team()
                    .AddPlayer(players[5], playerRatings[5])
                    .AddPlayer(players[6], playerRatings[6])
                    .AddPlayer(players[7], playerRatings[7])
                    .AddPlayer(players[8], playerRatings[8])
                    .AddPlayer(players[9], playerRatings[9]);


            // A "Team" is a collection of "Player" objects. Here we have a team
            // that consists of single players.

            // Note that for each player on the team, we indicate that they have
            // the "DefaultRating" which means that the algorithm has never seen
            // them before. In a real implementation, you'd pull this previous
            // rating for the player based on the player.Id value. It could come
            // from a database.

            //Rating r1 = new Rating();



            // We bundle up all of our teams together so that we can feed them to
            // the algorithm.
            var teams = Teams.Concat(team1, team2);

            // Before we know the actual results of the game, we can ask the 
            // calculator for what it perceives as the quality of the match (higher
            // means more fair/equitable)
            //AssertMatchQuality(0.447, TrueSkillCalculator.CalculateMatchQuality(gameInfo, teams));

            // This is the key line. We ask the calculator to calculate new ratings
            // Pay careful attention to the numbers at the end. This indicates that
            // team1 came in first place and team2 came in second place. TrueSkill
            // is flexible and allows scenarios such as team1 and team2 drawing which
            // could be represented as "1,1" since they both came in first place.
            var teamOneValue = 2;
            var teamTwoValue = 2;
            if (_teamOneWinner)
            {
                teamOneValue = 1;
            }
            else if (_teamTwoWinner)
            {
                teamTwoValue = 1;
            }

            var newRatings = TrueSkillCalculator.CalculateNewRatings(gameInfo, teams, teamOneValue, teamTwoValue);

            // The result of the calculation is a dictionary mapping the players to
            // their new rating. Here we get the ratings out for each player


            var playerNewRatings = new Rating[10];

            for (var i = 0; i < 10; i++)
            {
                playerNewRatings[i] = newRatings[players[i]];

                switch (type)
                {
                    case "player":
                        data.ReplayPlayer[i].PlayerConservativeRating = playerNewRatings[i].ConservativeRating;
                        data.ReplayPlayer[i].PlayerMean = playerNewRatings[i].Mean;
                        data.ReplayPlayer[i].PlayerStandardDeviation = playerNewRatings[i].StandardDeviation;
                        break;
                    case "role":
                        data.ReplayPlayer[i].RoleConservativeRating = playerNewRatings[i].ConservativeRating;
                        data.ReplayPlayer[i].RoleMean = playerNewRatings[i].Mean;
                        data.ReplayPlayer[i].RoleStandardDeviation = playerNewRatings[i].StandardDeviation;
                        break;
                    case "hero":
                        data.ReplayPlayer[i].HeroConservativeRating = playerNewRatings[i].ConservativeRating;
                        data.ReplayPlayer[i].HeroMean = playerNewRatings[i].Mean;
                        data.ReplayPlayer[i].HeroStandardDeviation = playerNewRatings[i].StandardDeviation;
                        break;
                }
            }

            return data;
        }
    }
}
