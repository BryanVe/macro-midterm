package midterm.p3;

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
    // Inicializamos el mayor y menor salario
    int minSalaryInUSD = Integer.MAX_VALUE;
    int maxSalaryInUSD = Integer.MIN_VALUE;

    while (iterator.hasNext()) {
      Text value = iterator.next();
      int salaryInUSD = Integer.parseInt(value.toString());

      // Actualizamos el mínimo y máximo en caso se cumpla las condiciones
      if (salaryInUSD < minSalaryInUSD) {
        minSalaryInUSD = salaryInUSD;
      }
      if (salaryInUSD > maxSalaryInUSD) {
        maxSalaryInUSD = salaryInUSD;
      }
    }

    // Obtenemos el rango salarial en USD
    String range = "[" + minSalaryInUSD + " - " + maxSalaryInUSD + "] USD";
    Text newValue = new Text(range);

    // Devolvemos la key que es el tamaño y el rango salarial en USD como valor
    outputCollector.collect(key, newValue);
  }
}
