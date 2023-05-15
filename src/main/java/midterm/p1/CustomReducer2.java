package midterm.p1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // Inicializamos una suma y frecuencia en 0, para
    // posteriormente obtener el promedio del salario en USD por país
    int sum = 0;
    int frequency = 0;

    while (iterator.hasNext()) {
      Text value = iterator.next();
      int salaryInUSD = Integer.parseInt(value.toString());

      // Realizamos la suma y aumentamos el contador
      sum += salaryInUSD;
      frequency += 1;
    }

    // Obtenemos el promedio del salario en USD
    float average = (float) sum / frequency;
    Text newValue = new Text(Float.toString(average));

    // Devolvemos la key que es el país y el promedio como valor
    outputCollector.collect(key, newValue);
  }
}
