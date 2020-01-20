using System;
using System.Collections.Generic;
using MMR_Globals_Calculator.Models;
using MySql.Data.MySqlClient;
using Moserware.Skills;

namespace MMR_Globals_Calculator
{
    public class MmrCalculator
    {
        private readonly DbSettings _dbSettings;
        private const double ErrorTolerance = 0.085;

        private string _type;
        private string _typeId;
        public ReplayData Data;

        private bool _teamOneWinner = false;
        private bool _teamTwoWinner = false;
        private double[] _playerMmRs = new double[10];
        private double[] _playerConserv = new double[10];
        private Dictionary<string, string> _mmrIds;
        private Dictionary<string, string> _role;

        public MmrCalculator(ReplayData data, string type, Dictionary<string, string> mmrIds, Dictionary<string, string> role, 
                             DbSettings dbSettings)
        {
            _dbSettings = dbSettings;
            _type = type;
            Data = data;
            _mmrIds = mmrIds;
            _role = role;
            TwoPlayerTestNotDrawn();

        }

        private void TwoPlayerTestNotDrawn()
        {
            using var conn = new MySqlConnection(_dbSettings.ConnectionString);
            conn.Open();
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
                using var cmd = conn.CreateCommand();
                _typeId = _type switch
                {
                        "player" => _mmrIds["player"],
                        "hero" => _mmrIds[Data.Replay_Player[i].Hero],
                        "role" => _mmrIds[_role[Data.Replay_Player[i].Hero]],
                        _ => _typeId
                };

                cmd.CommandText = "SELECT * FROM master_mmr_data WHERE type_value = " + _typeId + " AND game_type = " + Data.GameType_id + "  AND blizz_id = " + Data.Replay_Player[i].BlizzId +
                                  " AND region = " + Data.Region;
                //Console.WriteLine(cmd.CommandText);
                var reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    _playerConserv[i] = Convert.ToDouble(reader.GetString("conservative_rating"));
                    playerRatings[i] = new Rating(Convert.ToDouble(reader.GetString("mean")), Convert.ToDouble(reader.GetString("standard_deviation")));
                }
            }

            _teamOneWinner = Data.Replay_Player[0].Winner;
            _teamTwoWinner = Data.Replay_Player[9].Winner;

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

                switch (_type)
                {
                    case "player":
                        Data.Replay_Player[i].player_conservative_rating = playerNewRatings[i].ConservativeRating;
                        Data.Replay_Player[i].player_mean = playerNewRatings[i].Mean;
                        Data.Replay_Player[i].player_standard_deviation = playerNewRatings[i].StandardDeviation;
                        break;
                    case "role":
                        Data.Replay_Player[i].role_conservative_rating = playerNewRatings[i].ConservativeRating;
                        Data.Replay_Player[i].role_mean = playerNewRatings[i].Mean;
                        Data.Replay_Player[i].role_standard_deviation = playerNewRatings[i].StandardDeviation;
                        break;
                    case "hero":
                        Data.Replay_Player[i].hero_conservative_rating = playerNewRatings[i].ConservativeRating;
                        Data.Replay_Player[i].hero_mean = playerNewRatings[i].Mean;
                        Data.Replay_Player[i].hero_standard_deviation = playerNewRatings[i].StandardDeviation;
                        break;
                }
            }
        }
    }
}
