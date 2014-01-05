using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Streaminvi;
using TwitterToken;
using TweetinCore;
using TweetinCore.Interfaces.TwitterToken;
using TweetinCore.Interfaces;
using System.Timers;
using System.IO;
using System.Diagnostics;

namespace TwitterStreaming
{
    class TwitterStreamer
    {
        string ConsumerKey1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerKey1"];
        string ConsumerSecret1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerSecret1"];
        string AccessToken1 = System.Configuration.ConfigurationManager.AppSettings["AccessToken1"];
        string AccessTokenSecret1 = System.Configuration.ConfigurationManager.AppSettings["AccessTokenSecret1"];

        private static readonly string dbName = "TweetsDB";
        internal static readonly string connStringInitial = "Server=TOKASHYOS-PC\\SQLEXPRESS;User Id=sa;Password=tokash30;database=master";
        internal static readonly string connString = "Server=TOKASHYOS-PC\\SQLEXPRESS;User Id=sa;Password=tokash30;database=" + dbName;
        
        public static string TweetsTableSchema = @"(TweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), Tweet nvarchar (4000), TimeOfTweet nvarchar (40))";
        internal static readonly string[] TweetsTableColumns = { "TweetID", "UserID", "Tweet", "TimeOfTweet"};

        public static string ReTweetsTableSchema = @"(ReTweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), SourceTweetID nvarchar (25), SourceUserID nvarchar (25), TimeOfReTweet nvarchar (40))";
        internal static readonly string[] RetweetsTableColumns = { "ReTweetID", "UserID", "SourceTweetID", "SourceUserID", "TimeOfReTweet" };

        private static readonly string[] tableNames = { "Tweets", "Retweets" };
        private static readonly string[] tableSchemas = { "CREATE TABLE Tweets (TweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), Tweet nvarchar (4000), TimeOfTweet nvarchar (40))",
                                                          "CREATE TABLE Retweets (ReTweetID nvarchar (25) PRIMARY KEY, UserID nvarchar (25), SourceTweetID nvarchar (25), SourceUserID nvarchar (25), TimeOfReTweet nvarchar (40))"};

        private static readonly string sqlCommandCreateDB = "CREATE DATABASE " + dbName + " ON PRIMARY " +
                "(NAME = " + dbName + ", " +
                "FILENAME = 'D:\\" + dbName + ".mdf', " +
                "SIZE = 3MB, MAXSIZE = 10MB, FILEGROWTH = 10%) " +
                "LOG ON (NAME = " + dbName + "_LOG, " +
                "FILENAME = 'D:\\" + dbName + ".ldf', " +
                "SIZE = 1MB, " +
                "MAXSIZE = 100MB, " +
                "FILEGROWTH = 10%)";

        #region Members
        SimpleStream _SimpleStream;
        FilteredStream _FilteredStream;

        List<TweetinCore.Interfaces.ITweet> _Tweets = new List<TweetinCore.Interfaces.ITweet>();
        //UserStream _UserStream;
        IToken _Token;
        Timer _Timer = new Timer();
        bool _IsStreamStopped = false;
        Action<TweetinCore.Interfaces.ITweet, string> _FileDataHandlerMethod;
        Action<TweetinCore.Interfaces.ITweet, int> _DataHandlerMethod;
        #endregion

        /// <summary>
        /// C'tor
        /// </summary>
        /// <param name="iUrl">The URL to stream from</param>
        /// <param name="iPeriod">The period of time in miliseconds in which streaming will occur</param>
        public TwitterStreamer(string iUrl, double iPeriod )
        {
            _Token = new Token(AccessToken1, AccessTokenSecret1, ConsumerKey1, ConsumerSecret1);
            _SimpleStream = new SimpleStream(iUrl);
            
            //_FilteredStream = new FilteredStream();

            //_FilteredStream.AddTrack("#android");
            //_UserStream = new UserStream();
            _Timer.Interval = iPeriod;
            _Timer.AutoReset = true; //Stops it from repeating
            //_Timer.Start();
            _Timer.Elapsed += new ElapsedEventHandler(TimerElapsed);

            CreateEmptyDB();
        }

        public void GetStreamIntoFile()
        {
            string filename = "Stream_" + DateTime.Now.ToString("dd.MM.yyyy.HH.mm.ss.ffff");
            _FileDataHandlerMethod = HandleTweet;

            _SimpleStream.StartStream(_Token, x => _FileDataHandlerMethod(x, filename));
            //_FilteredStream.StartStream(_Token, x => _DataHandlerMethod(x, filename));
        }

        public void GetTweets(int iNumTweets)
        {
            _DataHandlerMethod = GetTweetsHandler;
            Stopwatch timer = new Stopwatch();

            timer.Start();
            Console.WriteLine("Started looking for tweets...");
            _SimpleStream.StartStream(_Token, x => _DataHandlerMethod(x, iNumTweets));
            Console.WriteLine("Finished looking for tweets...");
            timer.Stop();
            Console.WriteLine(String.Format("Looking for tweets took: {0}", timer.Elapsed));

            int i = 0;
            foreach (ITweet tweet in _Tweets)
	        {
		        Console.WriteLine(String.Format("Started getting retweets for tweet:...", tweet.IdStr));
                tweet.Retweets = tweet.GetRetweets();
                Console.WriteLine(String.Format("Finished getting retweets for tweet:...", tweet.IdStr));

                i++;
                Console.WriteLine(String.Format("Tweets to go: {0}", _Tweets.Count - i));
	        }

            Console.WriteLine("Adding tweets to DB...");
            foreach (ITweet tweet in _Tweets)
            {
                AddTweetToDB(tweet);
            }
            Console.WriteLine("Finished adding tweets to DB...");

        }

        public void PrintTweets()
        {
            foreach (ITweet tweet in _Tweets)
            {
                string s = string.Format("Twitter: {0}\nMessage: {1}\nDate: {2}\n", tweet.Creator.Name, tweet.Text, tweet.CreatedAt);
                Console.WriteLine(s);
            }
        }

        //{ "TweetID", "UserID", "Tweet", "TimeOfTweet"};
        private void AddTweetToDB(ITweet iTweet)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", TweetsTableSchema[0]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[1]), iTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[2]), iTweet.Text);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[3]), iTweet.CreatedAt.ToString());
            
            try
            {
                SQLServerCommon.SQLServerCommon.Insert("Tweets", connString, TweetsTableColumns, parameters);
            }
            catch (Exception)
            {

                throw;
            }

            foreach (ITweet retweet in iTweet.Retweets)
            {
                AddReTweetToDB(retweet, iTweet);
            }
        }

        //{ "ReTweetID", "UserID", "SourceTweetID", "SourceUserID", "TimeOfReTweet" };
        private void AddReTweetToDB(ITweet iReTweet, ITweet iTweet)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", ReTweetsTableSchema[0]), iReTweet.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[1]), iReTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[2]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[3]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableSchema[4]), iReTweet.CreatedAt.ToString());


            try
            {
                SQLServerCommon.SQLServerCommon.Insert("Retweets", connString, RetweetsTableColumns, parameters);
            }
            catch (Exception)
            {

                throw;
            }
        }

        private void CreateEmptyDB()
        {
            int i = 0;
            try
            {
                //Create DB
                if (!SQLServerCommon.SQLServerCommon.IsDatabaseExists(connStringInitial, dbName))//connStringInitial, dbName))
                {
                    SQLServerCommon.SQLServerCommon.ExecuteNonQuery(sqlCommandCreateDB, connStringInitial);

                    foreach (string tableName in tableNames)
                    {
                        SQLServerCommon.SQLServerCommon.ExecuteNonQuery(tableSchemas[i], connString);
                    }
                        i++;
                }
                else
                {
                    //Check if all tables exist, if not, create them
                    
                    foreach (string tableName in tableNames)
                    {
                        if (SQLServerCommon.SQLServerCommon.IsTableExists(connString, dbName, tableName) == false)
                        {
                            SQLServerCommon.SQLServerCommon.ExecuteNonQuery(tableSchemas[i], connString);
                        }
                        i++;
                    }
                }

            }
            catch (Exception)
            {
                throw;
            }
        }

        private void GetTweetsHandler(TweetinCore.Interfaces.ITweet iTweet, int iNumTweets)
        {
            if (_Tweets.Count < iNumTweets)
            {
                if (iTweet.Hashtags.Count > 0 && !iTweet.Text.StartsWith("RT") && iTweet.Retweeted == true)
                {
                    _Tweets.Add(iTweet);
                }
            }
            else
            {
                _SimpleStream.StopStream();
            }
        }

        private void HandleTweet(TweetinCore.Interfaces.ITweet iTweet, string iFileName)
        {
            using (StreamWriter writer = new StreamWriter(iFileName, true))
            {
                writer.WriteLine(iTweet.Text);
            }
        }

        void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            //_SimpleStream.StopStream();
            _FilteredStream.StopStream();
            _IsStreamStopped = true;
        }
        
    }
}
