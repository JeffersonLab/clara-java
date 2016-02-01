package org.jlab.clara.engine;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;


/**
 * Loads the service specification from a YAML file.
 */
public class EngineSpecification {

    /**
     * Reports any problem parsing the service specification file.
     */
    public static class ParseException extends RuntimeException {
        public ParseException() {
        }

        public ParseException(String message) {
            super(message);
        }

        public ParseException(Throwable cause) {
            super(cause);
        }

        public ParseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private String name;
    private String engine;
    private String type;

    private String author;
    private String email;

    private String version;
    private String description;


    /**
     * Uses the service class to detect the YAML file.
     *
     * Example:
     * <pre>
     * package std.services.Simple;
     *
     * class Simple extends JService {
     *     private CServiceSpecification info;
     *
     *     public Simple() {
     *         this.info = new CServiceSpecification(this.getClass());
     *     }
     *
     *     ...
     *
     *     public getName() {
     *         return info.name();
     *     }
     *
     *     ...
     * }
     * </pre>
     * will search for the file
     * <code>std/services/Simple.yaml</code> in the CLASSPATH.
     */
    public EngineSpecification(Class<?> c) {
        this(c.getName());
    }


    /**
     * Uses the full service engine class name to detect the YAML file.
     * <p>
     * Example:
     * <pre>
     *     new CServiceSpecification("std.services.convertors.EvioToEvioReader")
     * </pre>
     * will search for the file
     * <code>std/services/convertors/EvioToEvioReader.yaml</code> in the CLASSPATH.
     */
    @SuppressWarnings("unchecked")
    public EngineSpecification(String engine) {
        InputStream input = getSpecStream(engine);
        if (input != null) {
            Yaml yaml = new Yaml();
            try {
                Object content = yaml.load(input);
                if (content instanceof Map) {
                    parseContent((Map<String, Object>) content);
                } else {
                    throw new ParseException("Unexpected YAML content");
                }
            } catch (YAMLException e) {
                throw new ParseException(e);
            }
        } else {
            throw new ParseException("Service specification file not found for " + engine);
        }
    }


    private InputStream getSpecStream(String engine) {
        ClassLoader cl = getClass().getClassLoader();
        String descriptionFile = engine.replaceAll("\\.", File.separator) + ".yaml";
        return cl.getResourceAsStream(descriptionFile);
    }


    private void parseContent(Map<String, Object> content) {
        name = parseString(content, "name");
        engine = parseString(content, "engine");
        author = parseString(content, "author");
        email = parseString(content, "email");
        type = parseString(content, "type");
        version = parseString(content, "version");
        description = parseString(content, "description");
    }


    private String parseString(Map<String, Object> content, String key) {
        Object value = content.get(key);
        if (value == null) {
            throw new ParseException("Missing key: " + key);
        }
        if (value instanceof String) {
            return (String) value;
        } else if (value instanceof Integer || value instanceof Double) {
            return value.toString();
        } else {
            throw new ParseException("Bad type for: " + key);
        }
    }


    public String name() {
        return name;
    }


    public String engine() {
        return engine;
    }


    public String type() {
        return type;
    }


    public String author() {
        return author;
    }


    public String email() {
        return email;
    }


    public String version() {
        return version;
    }


    public String description() {
        return description;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NAME:\n    " + name + "\n\n");
        sb.append("ENGINE:\n    " + engine + "\n\n");
        sb.append("TYPE:\n    " + type + "\n\n");
        sb.append("VERSION:\n    " + version + "\n\n");
        sb.append("AUTHOR:\n    " + author + " <" + email + ">\n\n");
        sb.append("DESCRIPTION:\n    " + description);
        return sb.toString();
    }
}
