import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.HashtagEntity


object TwitterSentimentMain {

  def main(args: Array[String]) {

    val filters = Array("#BuffaloBills","#Bills","#MiamiDolphins","#Dolphins","#ThePhins","#NewEnglandPatriots","#ThePats","#Patriots","#NewYorkJets","#Jets","#GangGreen",
                         "#BaltimoreRavens","#Ravens","#CincinnatiBengals","#Bengals","#ClevelandBrowns","#Browns","#PittsburghSteelers","#Steelers",
                          "#HoustonTexans","#IndianapolisColts","#Colts","#BaltimoreColts","#JacksonvilleJaguars","#Jaguars","#Jags","#JacksonvilleJags","#TennesseeTitans","#Titans","#DenverBroncos","#Broncos","#OrangeCrush",
                          "#KCChiefs","#Chiefs","#KansasCityChiefs","#OaklandRaiders","#Raiders","#RaiderNation","#SanDiegoChargers","#Chargers","#SDChargers","#TheBolts","#SanDiegoSuperChargers","#DallasCowboys","#Cowboys","#AmericasTeam","#DoomsdayDefence",
                            "#NewYorkGiants","#Giants","#BigBlue","#GMen","#Jints","#BigBlueWreckingCrew","#NewYorkFootballGiants","#PhiladelphiaEagles","#Birdgang","#BlitzInc","#BirdsofPrey","#WashingtonRedskin","#Redskin","#TheSkins","#TheBurgudyandGold",
                            "#ChicagoBears","#DaBears","#TheMonstersoftheMidway","#DetroitLions","#Lions","#GreenBayPackers","#Packers","#IndianPackers","#Blues","#BigBayBlues","#Bays","#ThePack","#TheGreenandGold","#MinnesotaVikings","#Vikings","#TheVikes","#ThePurple","#PurplePride","#ThePurplePeopleEaters","#ThePurpleandGold",
                            "#AtlantaFalcons","#Falcons","#TheDirtyBirds","#CarolinaPanthers","#Panthers","#NewOrleansSaints","#Saints","#BlackandGold","#NoiretOr","#TheWhoDatNation","#TheBayouBoys","#TheDomePatrol","#TheWhoDatsBoys","#TheBlessYouBoys","#TheCajunKids","#TheBeastsoftheBayou",
                            "#TampaBayBuccaneers","#Buccaneers","#TheBucs","#ArizonaCardinals","#Cardinals","#TheCards","#BigRed","#StLouisRams","#TheGreatestShowonTurf","#SanFrancisco49ers","#49ers","#Niners","#SeattleSeahawks","#Seahawks","#TheHawks","#TheBlueWave","#TheLegionofBoom"
                        )
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials

    System.setProperty("twitter4j.oauth.consumerKey", "${twitter_consumer_key}")
    System.setProperty("twitter4j.oauth.consumerSecret", "${twitter_consumer_secret}")
    System.setProperty("twitter4j.oauth.accessToken", "${twitter_access_token}")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "${twitter_access_token_secret}")

    //Create a spark configuration with a custom name and master
    // For more master configuration see  https://spark.apache.org/docs/1.2.0/submitting-applications.html#master-urls
    val sparkConf = new SparkConf().setAppName("STweetsApp").setMaster("local[*]")
    //Create a Streaming COntext with 2 second window
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //Using the streaming context, open a twitter stream (By the way you can also use filters)
    //Stream generates a series of random tweets
    val stream = TwitterUtils.createStream(ssc, None, filters)


    val sentiment:DStream[TweetWithSentiment]=stream.map{Status=> {
      val sa= new SentimentAnalyzer()
      val ttext= Status.getText()
      val date = Status.getCreatedAt()
      val location = Status.getGeoLocation();
      var loc="";
      if(location!=null) {
        val lat=location.getLatitude()
        val long = location.getLongitude()
        loc = lat.toString() + long.toString();
    }
      val tsource = Status.getSource
      val tweetsource = sa.sourceextractor(tsource)
      val sname = Status.getUser().getScreenName
      val uname = Status.getUser().getName()
      val lang = Status.getUser().getLang()
      val timezone = Status.getUser().getTimeZone()
      val retweetcount = Status.getRetweetCount()
      val folcount = Status.getUser().getFollowersCount()
      val frcount = Status.getUser().getFriendsCount()
      var hash = new Array[HashtagEntity](10)
      hash = Status.getHashtagEntities()
      var hashtag = new Array[String](10)
      hashtag = hash.map(_.getText)
      var teamtag = ""

      if(ttext.contains("#BuffaloBills") || ttext.contains("#Bills")) {
        teamtag = "BB"
      }
      else if(ttext.contains("#MiamiDolphins") || ttext.contains("#Dolphins") || ttext.contains("#ThePhins")) {
        teamtag = "MD"
      }
      else if(ttext.contains("#NewEnglandPatriots") || ttext.contains("#ThePats") || ttext.contains("#Patriots")) {
        teamtag = "NEP"
      }
      else if(ttext.contains("#NewYorkJets") || ttext.contains("#Jets") || ttext.contains("#GangGreen")) {
        teamtag = "NYJ"
      }
      else if(ttext.contains("#BaltimoreRavens") || ttext.contains("#Ravens")) {
        teamtag = "BR"
      }
      else if(ttext.contains("#ClevelandBrowns") || ttext.contains("#Browns")) {
        teamtag = "CLB"
      }
      else if(ttext.contains("#CincinnatiBengals") || ttext.contains("#Bengals")) {
        teamtag = "CIB"
      }
      else if(ttext.contains("#PittsburghSteelers") || ttext.contains("#Steelers")) {
        teamtag = "PS"
      }
      else if(ttext.contains("#HoustonTexans")) {
        teamtag = "HT"
      }
      else if(ttext.contains("#IndianapolisColts") || ttext.contains("#Colts") || ttext.contains("#BaltimoreColts")) {
        teamtag = "IC"
      }
      else if(ttext.contains("#JacksonvilleJaguars") || ttext.contains("#Jaguars") || ttext.contains("#Jags") || ttext.contains("#JacksonvilleJags")) {
        teamtag = "JJ"
      }
      else if(ttext.contains("#TennesseeTitans") || ttext.contains("#Titans")) {
        teamtag = "TT"
      }
      else if(ttext.contains("#DenverBroncos") || ttext.contains("#Broncos") || ttext.contains("#OrangeCrush")) {
        teamtag = "DB"
      }
      else if(ttext.contains("#KCChiefs") || ttext.contains("#Chiefs") || ttext.contains("#KansasCityChiefs")) {
        teamtag = "KCC"
      }
      else if(ttext.contains("#OaklandRaiders") || ttext.contains("#Raiders") || ttext.contains("#RaiderNation")) {
        teamtag = "OR"
      }

      else if(ttext.contains("#SanDiegoChargers") || ttext.contains("#SanDiegoSuperChargers") || ttext.contains("#TheBolts") || ttext.contains("#SDChargers") || ttext.contains("#Chargers")) {
        teamtag = "SDG"
      }
      else if(ttext.contains("#DallasCowboys") || ttext.contains("#AmericasTeam") || ttext.contains("#DoomsdayDefence") || ttext.contains("#Cowboys")){
        teamtag = "DC"
      }
      else if (ttext.contains("#NewYorkGiants") || ttext.contains("#BigBlue") || ttext.contains("#GMen") || ttext.contains("#Jints") || ttext.contains("#BigBlueWreckingCrew") || ttext.contains("#NewYorkFootballGiants") || ttext.contains("#Giants"))
      {
        teamtag = "NYG"
      }
      else if(ttext.contains("#PhiladelphiaEagles") || ttext.contains("#Birdgang") || ttext.contains("#BlitzInc") || ttext.contains("#BirdsofPrey"))
      {
        teamtag = "PE"
      }
      else if (ttext.contains("#WashingtonRedskin") || ttext.contains("#TheSkins") || ttext.contains("#TheBurgudyandGold") || ttext.contains("#Redskin")){
        teamtag = "WRS"
      }
      else if (ttext.contains("#ChicagoBears") || ttext.contains("#DaBears") || ttext.contains("#TheMonstersoftheMidway")){
        teamtag = "CHB"
      }
      else if(ttext.contains("#DetroitLions") || ttext.contains("#Lions")){
        teamtag = "DL"
      }
      else if (ttext.contains("#GreenBayPackers") || ttext.contains("#IndianPackers") || ttext.contains("#Blues") || ttext.contains("#BigBayBlues") || ttext.contains("#Bays") || ttext.contains("#ThePack") || ttext.contains("#TheGreenandGold") || ttext.contains("#Packers")){
        teamtag = "GBP"
      }
      else if (ttext.contains("#MinnesotaVikings") || ttext.contains("#TheVikes") || ttext.contains("#ThePurple") || ttext.contains("#PurplePride") || ttext.contains("#ThePurplePeopleEaters") || ttext.contains("#ThePurpleandGold") || ttext.contains("#Vikings")){
        teamtag = "MV"
      }
      else if (ttext.contains("#AtlantaFalcons") || ttext.contains("#TheDirtyBirds") || ttext.contains("#Falcons")){
        teamtag = "AF"
      }
      else if (ttext.contains("#CarolinaPanthers") || ttext.contains("#Panthers")){
        teamtag = "CP"
      }
      else if (ttext.contains("#NewOrleansSaints") || ttext.contains("#BlackandGold") || ttext.contains("#NoiretOr") || ttext.contains("#TheWhoDatNation") || ttext.contains("#TheBayouBoys") || ttext.contains("#TheDomePatrol") || ttext.contains("#TheWhoDatsBoys") || ttext.contains("#TheBlessYouBoys") || ttext.contains("#TheCajunKids") || ttext.contains("#TheBeastsoftheBayou") || ttext.contains("#Saints")){
        teamtag = "NOS"
      }
      else if(ttext.contains("#TampaBayBuccaneers") || ttext.contains("#TheBucs") || ttext.contains("#Buccaneers")){
        teamtag = "TBB"
      }
      else if (ttext.contains("#ArizonaCardinals") || ttext.contains("#TheCards") || ttext.contains("#BigRed") || ttext.contains("#Cardinals")){
        teamtag = "AC"
      }
      else if (ttext.contains("#StLouisRams") || ttext.contains("#TheGreatestShowonTurf")|| ttext.contains("#Rams")){
        teamtag = "SLR"
      }
      else if (ttext.contains("#SanFrancisco49ers") || ttext.contains("#Niners") || ttext.contains("#49ers")){
        teamtag = "SF49"
      }
      else if (ttext.contains("#SeattleSeahawks") || ttext.contains("#TheHawks") || ttext.contains("#TheBlueWave") || ttext.contains("#TheLegionofBoom") || ttext.contains("#Seahawks")){
        teamtag = "SSH"
      }

      /*
      val place = Status.getPlace()
      if (place != null) {
      val name = place.getCountry()
      val name2 = place.getName()
      print(name)
      print(name2)
    }
*/
      //place.
     // print(place);
     // print(location);
    //  print(st);
    //  print(date);


      val tw=sa.findSentiment(ttext,date,hashtag,tweetsource,sname,uname,lang,timezone,retweetcount,folcount,frcount,teamtag)
      tw
    }}

   sentiment.foreachRDD{
      rdd=>rdd.foreach{
        tw=> {

        }}}

    ssc.start()

    ssc.awaitTermination()
  }

}
