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

        // 4) Prepare output file
        Path outPath = Paths.get("dmetaphone.test");
        try (var writer = Files.newBufferedWriter(outPath)) {
            int idx = 1;
            for (String name : names) {
                String primary   = dm.doubleMetaphone(name);
                String alternate = dm.doubleMetaphone(name, true);

                // Build array-like representation
                String arrayOutput = (!alternate.isEmpty() && !alternate.equals(primary))
                        ? String.format("[%s, %s]", primary, alternate)
                        : String.format("[%s]", primary);

                // Write block
                writer.write(String.format("query I%n", idx++));
                writer.write(String.format("SELECT double_metaphone('%s');%n", name));
                writer.write("----\n");
                writer.write(arrayOutput);
                writer.write("\n\n");  // blank line
            }
        }

        System.out.println("Done! Wrote " + names.size() + " entries to " + outPath.toAbsolutePath());
    }
}
