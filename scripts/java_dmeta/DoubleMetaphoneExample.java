import org.apache.commons.codec.language.DoubleMetaphone;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class DoubleMetaphoneExample {

    // Maximum number of names to process
    private static final int MAX = 1000;

    public static void main(String[] args) throws IOException {
        // 1) Load all names (one per line) from CSV
        List<String> allNames = Files.lines(Paths.get("wikidata_names.csv"))
                                     .map(String::trim)
                                     .filter(line -> !line.isEmpty())
                                     .collect(Collectors.toList());

        // 2) Limit to first MAX names
        List<String> names = allNames.size() > MAX
                ? allNames.subList(0, MAX)
                : allNames;

        // 3) Prepare DoubleMetaphone
        DoubleMetaphone dm = new DoubleMetaphone();
        dm.setMaxCodeLen(6);

        // 4) Prepare output files: .test and .csv
        Path testPath = Paths.get("dmetaphone.test");
        Path csvPath = Paths.get("dmetaphone.csv");
        try (var testWriter = Files.newBufferedWriter(testPath);
             var csvWriter = Files.newBufferedWriter(csvPath)) {

            // Write CSV header
            csvWriter.write("input,expected");
            csvWriter.newLine();

            for (String name : names) {
                String primary   = dm.doubleMetaphone(name);
                String alternate = dm.doubleMetaphone(name, true);

                // Build array-like representation
            String p = primary.contains(" ")
                ? String.format("'%s'", primary)
                : primary;
            String arrayOutput;
            if (!alternate.isEmpty() && !alternate.equals(primary)) {
                String a = alternate.contains(" ")
                    ? String.format("'%s'", alternate)
                    : alternate;
                arrayOutput = String.format("[%s, %s]", p, a);
            } else {
                arrayOutput = String.format("[%s]", p);
            }

                // Write to .test file
                testWriter.write("query I");
                testWriter.newLine();
                testWriter.write(String.format("SELECT double_metaphone('%s');%n", name.replace("'", "''")));
                testWriter.write("----");
                testWriter.newLine();
                testWriter.write(arrayOutput);
                testWriter.newLine();
                testWriter.newLine();

                // Write to CSV: wrap expected in quotes
                csvWriter.write(String.format("%s,\"%s\"", name, arrayOutput));
                csvWriter.newLine();
            }
        }

        System.out.println("Done! Wrote " + names.size() + " entries to " + testPath.toAbsolutePath());
        System.out.println("CSV written to " + csvPath.toAbsolutePath());

        String input = "Hemptah"; // hard-coded input

        DoubleMetaphone dm2 = new DoubleMetaphone();
        dm2.setMaxCodeLen(6); // optional, 4 is default

        String primary = dm2.doubleMetaphone(input);
        String alternate = dm2.doubleMetaphone(input, true);

        System.out.println("Input: " + input);
        System.out.println("Primary: " + primary);
        System.out.println("Alternate: " + alternate);
    }
}