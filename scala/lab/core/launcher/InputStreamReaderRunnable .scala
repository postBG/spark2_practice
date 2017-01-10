package lab.core.launcher

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.util.Date

class InputStreamReaderRunnable(val is: InputStream, val name: String) extends Runnable {
  
  val reader: BufferedReader = new BufferedReader(new InputStreamReader(is));
  
  def run() = {
    val prefix = ">>>> InputStream " + name + " : "
    try {
        var line = reader.readLine();
        while (line != null) {
            println(prefix + line);
            line = reader.readLine();
        }
        reader.close();
    } catch {
      case e: Exception => e.printStackTrace();
    }
  }
}