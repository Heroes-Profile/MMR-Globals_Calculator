using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using Moserware.Skills;
using NUnit.Framework;

namespace MMR_Globals_Calculator
{
    public class MMRCalculator
    {
        private string db_connect_string = new DB_Connect().heroesprofile_config;
        private const double ErrorTolerance = 0.085;

        private string type;
        private string type_id;
        public ReplayData data;

        Boolean teamOneWinner = false;
        Boolean teamTwoWinner = false;
        double[] playerMMRs = new double[10];
        double[] playerConserv = new double[10];
        private Dictionary<string, string> mmr_ids = new Dictionary<string, string>();
        private Dictionary<string, string> role = new Dictionary<string, string>();

        public MMRCalculator(ReplayData data, string type, Dictionary<string, string> mmr_ids, Dictionary<string, string> role)
        {

            this.type = type;
            this.data = data;
            this.mmr_ids = mmr_ids;
            this.role = role;
            TwoPlayerTestNotDrawn();

        }

        private void TwoPlayerTestNotDrawn()
        {
            using (MySqlConnection conn = new MySqlConnection(db_connect_string))
            {
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
                Rating[] playerRatings = new Rating[10];
                for (int i = 0; i < 10; i++)
                {
                    playerMMRs[i] = 0;
                    playerConserv[i] = 0;
                    playerRatings[i] = gameInfo.DefaultRating;
                }

                for (int i = 0; i < 10; i++)
                {
                    using (MySqlCommand cmd = conn.CreateCommand())
                    {
                        if (type == "player")
                        {
                            type_id = mmr_ids["player"];
                        }
                        else if (type == "hero")
                        {
                            type_id = mmr_ids[data.Replay_Player[i].Hero];
                        }
                        else if (type == "role")
                        {
                            type_id = mmr_ids[role[data.Replay_Player[i].Hero]];

                        }
                        cmd.CommandText = "SELECT * FROM master_mmr_data WHERE type_value = " + type_id + " AND game_type = " + data.GameType_id + "  AND blizz_id = " + data.Replay_Player[i].BlizzId + " AND region = " + data.Region;
                        //Console.WriteLine(cmd.CommandText);
                        MySqlDataReader Reader = cmd.ExecuteReader();

                        while (Reader.Read())
                        {

                            playerConserv[i] = Convert.ToDouble(Reader.GetString("conservative_rating"));
                            playerRatings[i] = new Rating(Convert.ToDouble(Reader.GetString("mean")), Convert.ToDouble(Reader.GetString("standard_deviation")));

                        }
                    }
                }
                teamOneWinner = data.Replay_Player[0].Winner;
                teamTwoWinner = data.Replay_Player[9].Winner;

                Player[] players = new Player[10];

                for (int i = 0; i < 10; i++)
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
                int teamOneValue = 2;
                int teamTwoValue = 2;
                if (teamOneWinner)
                {
                    teamOneValue = 1;
                }
                else if (teamTwoWinner)
                {
                    teamTwoValue = 1;
                }
                var newRatings = TrueSkillCalculator.CalculateNewRatings(gameInfo, teams, teamOneValue, teamTwoValue);

                // The result of the calculation is a dictionary mapping the players to
                // their new rating. Here we get the ratings out for each player


                Rating[] playerNewRatings = new Rating[10];

                for (int i = 0; i < 10; i++)
                {
                    playerNewRatings[i] = newRatings[players[i]];

                    if (type == "player")
                    {
                        data.Replay_Player[i].player_conservative_rating = playerNewRatings[i].ConservativeRating;
                        data.Replay_Player[i].player_mean = playerNewRatings[i].Mean;
                        data.Replay_Player[i].player_standard_deviation = playerNewRatings[i].StandardDeviation;

                    }
                    else if (type == "role")
                    {
                        data.Replay_Player[i].role_conservative_rating = playerNewRatings[i].ConservativeRating;
                        data.Replay_Player[i].role_mean = playerNewRatings[i].Mean;
                        data.Replay_Player[i].role_standard_deviation = playerNewRatings[i].StandardDeviation;
                    }
                    else if (type == "hero")
                    {
                        data.Replay_Player[i].hero_conservative_rating = playerNewRatings[i].ConservativeRating;
                        data.Replay_Player[i].hero_mean = playerNewRatings[i].Mean;
                        data.Replay_Player[i].hero_standard_deviation = playerNewRatings[i].StandardDeviation;
                    }

                }
            }

        }
    }
}
