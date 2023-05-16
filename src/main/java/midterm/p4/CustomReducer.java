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

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // Ahorar agrupamos por país mediante el reducer
    Map<String, Integer> frequencies = new HashMap<>();

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String jobTitle = value.toString();

      // Y empezamos agrupar las frecuencias de los trabajos
      if (!frequencies.containsKey(jobTitle)) {
        frequencies.put(jobTitle, 1);
      } else {
        frequencies.put(jobTitle, frequencies.get(jobTitle) + 1);
      }
    }

    // Iteramos sobre las entradas de las frecuencias y por
    // cada una damos una salida donde la clave es el país
    // y el valor el trabajo más su frecuencia
    for (Map.Entry<String, Integer> entry : frequencies.entrySet()) {
      String job = entry.getKey();
      Integer frequency = entry.getValue();
      Text newValue = new Text("/" + job + "/" + frequency);

      outputCollector.collect(key, newValue);
    }
  }
}
