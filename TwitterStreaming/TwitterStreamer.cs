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
using System.Text.RegularExpressions;

namespace TwitterStreaming
{
    class TwitterStreamer
    {
        string ConsumerKey1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerKey1"];
        string ConsumerSecret1 = System.Configuration.ConfigurationManager.AppSettings["ConsumerSecret1"];
        string AccessToken1 = System.Configuration.ConfigurationManager.AppSettings["AccessToken1"];
        string AccessTokenSecret1 = System.Configuration.ConfigurationManager.AppSettings["AccessTokenSecret1"];

        private static readonly string dbName = "TweetsDB";
        private static readonly string sqlserverName = "TOKASHYO-PC";

        internal static readonly string connStringInitial = "Server=" + sqlserverName + "\\SQLEXPRESS;User Id=sa;Password=tokash30;database=master";
        internal static readonly string connString = "Server=" + sqlserverName + "\\SQLEXPRESS;User Id=sa;Password=tokash30;database=" + dbName;
        
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
            _SimpleStream.StreamStopped += new EventHandler<TweetinCore.Events.GenericEventArgs<Exception>>(OnStreamStopped);
            
            //_FilteredStream = new FilteredStream();

            //_FilteredStream.AddTrack("#android");
            //_UserStream = new UserStream();
            _Timer.Interval = iPeriod;
            _Timer.AutoReset = true; //Stops it from repeating
            _Timer.Start();
            _Timer.Elapsed += new ElapsedEventHandler(TimerElapsed);

            CreateEmptyDB();
        }

        private void OnStreamStopped(object sender, TweetinCore.Events.GenericEventArgs<Exception> e)
        {
            _IsStreamStopped = true;
            if (e.Value != null)
            {
                Console.WriteLine(e.Value.ToString());
            }
        }

        public void GetStreamIntoFile()
        {
            string filename = "Stream_" + DateTime.Now.ToString("dd.MM.yyyy.HH.mm.ss.ffff");
            _FileDataHandlerMethod = HandleTweet;

            _SimpleStream.StartStream(_Token, x => _FileDataHandlerMethod(x, filename));
            //_FilteredStream.StartStream(_Token, x => _DataHandlerMethod(x, filename));
        }

        public void GetTweets()
        {
            //_DataHandlerMethod = GetTweetsHandler;
            Stopwatch swTimer = new Stopwatch();

            swTimer.Start();
            Console.WriteLine(string.Format("{0}: Started looking for tweets...", DateTime.Now));


            while (_Tweets.Count < 100)
            {
                _SimpleStream.StartStream(_Token, tweet =>
                {
                    //if (_Tweets.Count < 10 /*iNumTweets*/)
                    //{
                    //if (tweet.Hashtags.Count > 0 && tweet.Text.StartsWith("RT") && tweet.Retweeting != null)
                    //{
                    if (tweet.Retweeting != null)
                    {
                        if ((int)tweet.Retweeting.RetweetCount > 10 && tweet.Retweeting.Hashtags.Count > 0)
                        {
                            if (!Regex.IsMatch(tweet.Text, "[^\u0000-\u0080]+"))
                            {
                                _Tweets.Add(tweet.Retweeting); 
                            }
                        }
                    }
                    //}
                    //}
                    //else
                    //{
                    //    _SimpleStream.StopStream();
                    //}
                });

                _Timer.Start();
            }
            
            Console.WriteLine("Finished looking for tweets...");
            swTimer.Stop();
            Console.WriteLine(String.Format("{0}: Looking for tweets took: {1}", DateTime.Now, swTimer.Elapsed));

            int i = 0;
            while (i < _Tweets.Count)
            {
                Console.WriteLine(String.Format("{0}: Started getting retweets for tweet:{1} ...", DateTime.Now, _Tweets[i].IdStr));
                try
                {
                    _Tweets[i].Retweets = _Tweets[i].GetRetweets(false, false, _Token);


                    Console.WriteLine(String.Format("{0}: Finished getting retweets for tweet:{1}...", DateTime.Now, _Tweets[i].IdStr));

                    Console.WriteLine("Adding tweet to DB...");
                    AddTweetToDB(_Tweets[i]);
                    Console.WriteLine("Tweet added to DB...");

                    Console.WriteLine(String.Format("Tweets to go: {0}", _Tweets.Count - i));
                    i++;
                }
                catch (Exception ex)
                {
                    //reached limit
                    if (ex.Message.Contains("429"))
                    {
                        Console.WriteLine(string.Format("{0}: Rate limit reached for: {1}, waiting 15 minutes...", DateTime.Now, _Tweets[i].IdStr));
                        System.Threading.Thread.Sleep(900000);
                    }
                    else
                    {
                        Console.WriteLine(ex.ToString());
                        i++;
                    }
                }
            }            

        }

        public void PrintTweets()
        {
            foreach (ITweet tweet in _Tweets)
            {
                string s = string.Format("Twitter: {0}\nMessage: {1}\nDate: {2}\n", tweet.Creator.Name, tweet.Text, tweet.CreatedAt);
                Console.WriteLine(s);
            }
        }

        private void AddTweetToDB(ITweet iTweet)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", TweetsTableColumns[0]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableColumns[1]), iTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", TweetsTableColumns[2]), iTweet.Text.Replace("\n", ""));
            parameters.Add(String.Format("@{0}", TweetsTableColumns[3]), iTweet.CreatedAt.ToString());
            
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

        private void AddReTweetToDB(ITweet iReTweet, ITweet iTweet)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            parameters.Add(String.Format("@{0}", RetweetsTableColumns[0]), iReTweet.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[1]), iReTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[2]), iTweet.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[3]), iTweet.Creator.IdStr);
            parameters.Add(String.Format("@{0}", RetweetsTableColumns[4]), iReTweet.CreatedAt.ToString());


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

        #region DataHandlers
        private void GetTweets2Handler(ITweet iTweet, int iNumTweets)
        {
            if (_IsStreamStopped == false)
            {
                if (_Tweets.Count < iNumTweets)
                {
                    if (iTweet.Hashtags.Count > 0 && iTweet.Text.StartsWith("RT") && iTweet.Retweeting != null)
                    {
                        if (iTweet.Retweeting.RetweetCount > 1)
                        {
                            _Tweets.Add(iTweet.Retweeting);
                        }
                    }

                    if (_Tweets.Count == iNumTweets)
                    {
                        _SimpleStream.StopStream();
                    }
                }
            } 
        }

        private void GetTweetsHandler(TweetinCore.Interfaces.ITweet iTweet, int iNumTweets)
        {
            if (_Tweets.Count < iNumTweets)
            {
                if (iTweet.Hashtags.Count > 0 && iTweet.Text.StartsWith("RT") && iTweet.Retweeting != null)
                {
                    if (iTweet.Retweeting.RetweetCount > 1)
                    {
                        _Tweets.Add(iTweet.Retweeting);
                    }
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
        #endregion

        #region EventsHandlers
        void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            _SimpleStream.StopStream();
            //_FilteredStream.StopStream();
            _IsStreamStopped = true;
        } 
        #endregion
        
    }
}
