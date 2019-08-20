import java.io._
import scala.io.Source
import scala.collection.mutable.ListBuffer

object Task1_V3 extends App {

  //FRAMEWORK START
  // Worker class, workers execute map and reduce tasks
  class Worker(var workerName: String, var workerStatus: String, var workerIterator: Iterator[String]) {

    println("Worker Created: " + workerName)
    //Worker starts calling the User created mapper
    def startMapTask(callback: (String, Iterator[String]) => List[(String, String)]): List[(String, String)] =
      {
        workerStatus = "in-progress"
        var userMapper = callback(workerName, workerIterator)
        //combiner
        workerStatus = "completed"

        return userMapper
      }
    //Worker starts calling the User created Reducer
    def startReduceTask(callback: (List[(String, List[(String, String)])]) => List[String], mapperResultList: List[(String, List[(String, String)])]): List[String] =
      {
        workerStatus = "in-progress"
        var userReducer = callback(mapperResultList)
        workerStatus = "completed"

        return userReducer
      }

  }
  //Framework Class, Master Node
  class MapReduceFW(var mapReduceTaskName: String) {

    var workerList = List[Worker]()
    //start reducer part
    def startReducer(userReducer: (List[(String, List[(String, String)])]) => List[String], mapperResultList: List[(String, List[(String, String)])], outputColumns: String, outputDestinationPath: String, outputDestinationFileName: String) {
      val reducerResult = getIdleWorker().startReduceTask(userReducer, mapperResultList)
      writeResults(outputDestinationPath, outputDestinationFileName, outputColumns, reducerResult)

    }
    //Start mapper part
    def startMappper(mapFilesPath: String, userMapper: (String, Iterator[String]) => List[(String, String)]): List[(String, List[(String, String)])] = {
   
      workerList = createWorkersList(mapFilesPath)
      //      for each file to process by mapper worker is crated
      val workersResults = workerList.par.map(x => x.startMapTask(userMapper))
      //combine data and group it
      val mapResult = workersResults.toList.flatten.groupBy(_._1)
   
      return mapResult.toList
    }
    // Get idle worker from list, if there are none then create
    def getIdleWorker(): Worker =
      {
        val idleWorkerList = workerList.filter(x => x.workerStatus == "idle" || x.workerStatus == "completed")

        if (idleWorkerList.size > 0) {
          return idleWorkerList(1)
        } else {
          //Iterator is as a placeholder
          println("NEW Worker Is Created, Not Enough Workers")
          return createWorker("New Worker", Iterator("1"))
        }
      }
    //creating list of workers
    def createWorkersList(folderPath: String): List[Worker] = {
      val fileListData = getFileData(folderPath)
      val workerList = fileListData.map(x => createWorker(x._1, x._2))

      return workerList
    }
    //Returns worker
    def createWorker(workerName: String, workerIterator: Iterator[String]): Worker =
      {
        val w = new Worker(workerName, "idle", workerIterator)

        return w
      }
    //Returns all files in directory
    def getFilePath(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }
    //Returns iterator of file data
    def getFileData(folderPath: String): List[(String, Iterator[String])] =
      {
        val fileList = getFilePath(folderPath)
        if (fileList.size == 0) {
          println("WARNING!!! FILE DIRECTORY IS EMPTY")
        }
        //Getting an iterator with K V tuple for mapping, where K is file name and V is file content iterator
        //For each file we return tuple
        return fileList.map(y => (y.getName, Source.fromFile(y).getLines()))

      }
    //Writes results to the file
    def writeResults(destination: String, fileName: String, headers: String, resultList: List[String]) {
      val checkDestination = new File(destination)
      if (checkDestination.exists() == false) {
        println(destination + "DOES NOT EXIST! ")
      } else {
        val file = new File(destination + "/" + fileName)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(headers + "\n")
        for (results <- resultList) {
          bw.write(results + "\n")
        }
        bw.close()
      }
      println("Result is written to file successfully!")
    }
    

  }

  //FRAMEWORK END

  //User created mappers

  //Task 1 Click mapper
  def mapClicksTask1(fileName: String, fileContent: Iterator[String]): List[(String, String)] =
    {
      //key: file name??
      //value: file content
      var mapListBuffer = new ListBuffer[(String, String)]()
      for (rows <- fileContent.drop(1)) {
        val row = rows.split(",").map(x => x.trim())
        mapListBuffer += ((row(0).toString(), "1"))
      }
      return mapListBuffer.toList
    }

  //Task2 User Mapper
  def mapUsersTask2(fileName: String, fileContent: Iterator[String]): List[(String, String)] =
    {
      var mapListBuffer = new ListBuffer[(String, String)]()

      for (rows <- fileContent.drop(1)) {

        val row = rows.split(",").map(x => x.trim)

        if (row.length == 2 && row(1) == "LT") {
          mapListBuffer += ((row(0), row(1)))
        } else if (row.length == 3 && row(2) == "LT") {
          mapListBuffer += ((row(0), row(2)))
        }
      }

      return mapListBuffer.toList
    }
  //Task 2 Click Mapper
  def mapClicksTask2(fileName: String, fileContent: Iterator[String]): List[(String, String)] =
    {

      var mapListBuffer = new ListBuffer[(String, String)]()

      for (rows <- fileContent.drop(1)) {
        val row = rows.split(",").map(x => x.trim)
        if (row.length == 4) {
          mapListBuffer += ((row(2), row(0) + "," + row(1) + "," + row(2) + "," + row(3)))
        } else if (row.length == 3) {
          mapListBuffer += ((row(1), row(0) + "," + "" + "," + row(1) + "," + row(2)))
        }
      }

      return mapListBuffer.toList
    }

  //User created Reducers
  //Task 1 reducer
  def reducerClicksTask1(KVMap: List[(String, List[(String, String)])]): List[String] = {

    var reducerListBuffer = new ListBuffer[(String)]()

    for ((k, v) <- KVMap) {
      reducerListBuffer += (k + "," + v.size.toString())
    }
    return reducerListBuffer.toList
  }
  //Task2 Reducer
  def reducerClicksTask2(KVMap: List[(String, List[(String, String)])]): List[String] =
    {

      var reducerListBuffer = new ListBuffer[(String)]()
      var clickListBuffer = new ListBuffer[(String, String)]()
      var userSet = Set[String]()
      //splitting data between users and clicks
      for ((key, value) <- KVMap) {
        for (valueData <- value) {

          if (valueData._2.size == 2) {
            userSet += valueData._1
          } else {
            clickListBuffer += valueData
          }
        }
      }
      val filteredClicks = clickListBuffer.toList.groupBy(_._1).filterKeys(userSet).toList

      for (clicksList <- filteredClicks) {
        for (clicks <- clicksList._2) {
          reducerListBuffer += clicks._2
        }
      }
      return reducerListBuffer.toList
    }

  //Run FrameWork
  //task1
  val MR1 = new MapReduceFW("Task1")

  val MR1ClickResult = MR1.startMappper("C:/hw/data/clicks/", mapClicksTask1)

  val MR1Reducer = MR1.startReducer(reducerClicksTask1, MR1ClickResult, "date,clicks", "C:/hw/data/total_clicks/", "result.csv")

  //task2
  val MR2 = new MapReduceFW("Task2")

  val MR2UserResult = MR2.startMappper("C:/hw/data/users/", mapUsersTask2)
  val MR2ClickResult = MR2.startMappper("C:/hw/data/clicks/", mapClicksTask2)

  val MR2CombinedResults = MR2UserResult ++ MR2ClickResult

  val MR2Reducer = MR2.startReducer(reducerClicksTask2, MR2CombinedResults, "date,screen,user,target", "C:/hw/data/filtered_clicks/", "result.csv")

}

