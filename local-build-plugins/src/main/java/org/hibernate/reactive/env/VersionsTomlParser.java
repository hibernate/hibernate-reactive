package org.hibernate.reactive.env;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Read the versions section of the library catalog
 */
public class VersionsTomlParser {

    private final Map<String, String> data = new HashMap<>();

    public VersionsTomlParser(File filePath) {
        parse( filePath );
    }

    private void parse(File filePath) {

        try ( BufferedReader reader = new BufferedReader( new FileReader( filePath ) ) ) {
            String line;
            String currentSection = null;
            while ( ( line = reader.readLine() ) != null ) {
                line = line.trim();

                // Skip comments and blank lines
                if ( line.isEmpty() || line.startsWith( "#" ) ) {
                    continue;
                }

                // Handle [section]
                if ( line.startsWith( "[" ) && line.endsWith( "]" ) ) {
                    currentSection = line.substring( 1, line.length() - 1 ).trim();
                    continue;
                }

                if ( "versions".equalsIgnoreCase( currentSection ) ) {
                    // Handle key = value
                    int equalsIndex = line.indexOf( '=' );
                    if ( equalsIndex == -1 ) {
                        continue;
                    }

                    String key = line.substring( 0, equalsIndex ).trim();
                    String value = line.substring( equalsIndex + 1 ).trim();

                    // Remove optional quotes around string values
                    if ( value.startsWith( "\"" ) && value.endsWith( "\"" ) ) {
                        value = value.substring( 1, value.length() - 1 );
                    }

                    data.put( key, value );
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException( e );
        }
    }

    /**
     * Read the value of the property in the versions section of a toml file
     */
    public String read(String property) {
        return data.get( property );
    }
}
