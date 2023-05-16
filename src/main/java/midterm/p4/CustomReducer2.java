package midterm.p4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CustomReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // Ahorar inicializamos la mayor frecuencia con su trabajo
    int maxFrequency = Integer.MIN_VALUE;
    String maxJobTitle = "";

    while (iterator.hasNext()) {
      String value = iterator.next().toString();
      String[] data = value.split("/");
      String jobTitle = data[0];
      int frequency = Integer.parseInt(data[1]);

      // Y actualizamos conforme vayamos encontrando uno con más frecuencia
      if (frequency > maxFrequency) {
        maxFrequency = frequency;
        maxJobTitle = jobTitle;
      }
    }

    // Finalmente damos como salida el país como clave y el trabajo con su frecuencia como valor
    Text newValue = new Text(maxJobTitle + " (" + maxFrequency + ")");
    outputCollector.collect(key, newValue);
  }
}
