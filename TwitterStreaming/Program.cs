using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;



namespace TwitterStreaming
{
    class Program
    {
        static void Main(string[] args)
        {
            TwitterStreamer twitterStreamer = new TwitterStreamer("https://stream.twitter.com/1.1/statuses/sample.json", 20000);

            twitterStreamer.GetStreamIntoFile();
        }
    }
}
