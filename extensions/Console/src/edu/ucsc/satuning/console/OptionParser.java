package edu.ucsc.satuning.console;

import edu.ucsc.satuning.spi.ErrorChecking;
import edu.ucsc.satuning.util.StringsUtil;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static edu.ucsc.satuning.console.Tag.addUnsavedOption;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class OptionParser implements Parser {
    private static final String SINGLE_MARK     = "-";
    private static final String DOUBLE_MARKS    = "--";
    private static final String YES             = "yes";
    private static final String NO              = "no";
    private static final String TRUE            = "true";
    private static final String FALSE           = "false";
    private static final String HASH            = "#";
    private static final char   EQUAL           = '=';
    private static final char   SPACE           = ' ';


    private static final Map<Class<?>, OptionHandler> HANDLERS = new HashMap<Class<?>, OptionHandler>();
    private static final ErrorChecking ERROR_CHECKING;
    static {
        ERROR_CHECKING = new ErrorChecking();
        if(!HANDLERS.isEmpty()) HANDLERS.clear();
        HANDLERS.put(boolean.class, new BooleanHandler());
        HANDLERS.put(Boolean.class, new BooleanHandler());
        HANDLERS.put(byte.class, new ByteHandler());
        HANDLERS.put(Byte.class, new ByteHandler());
        HANDLERS.put(short.class, new ShortHandler());
        HANDLERS.put(Short.class, new ShortHandler());
        HANDLERS.put(int.class, new IntegerHandler());
        HANDLERS.put(Integer.class, new IntegerHandler());
        HANDLERS.put(long.class, new LongHandler());
        HANDLERS.put(Long.class, new LongHandler());
        HANDLERS.put(float.class, new FloatHandler());
        HANDLERS.put(Float.class, new FloatHandler());
        HANDLERS.put(double.class, new DoubleHandler());
        HANDLERS.put(Double.class, new DoubleHandler());
        HANDLERS.put(String.class, new StringHandler());
        HANDLERS.put(File.class, new FileHandler());
    }


    private final Object                     optionSource;
    private final Map<String, Field>         optionMap;
    private final Map<Field, Object>         defaultOptionMap;
    private final Usage                      usage;
    private final AtomicBoolean              allSucess;

    /**
     * construct an {@code OptionParser} object.
     * @param optionSource
     *      the source of all of the command-line options.
     * @param usage
     *      display, if any violation is found, the usage of
     *      the api.
     */
    public OptionParser(Object optionSource, Usage usage){
        this.usage              = usage;
        this.optionSource       = optionSource;
        this.optionMap          = makeOptionMap();
        this.defaultOptionMap   = new HashMap<Field, Object>();
        this.allSucess          = new AtomicBoolean(true);
    }


    public boolean allSucess(){
        return allSucess.get();
    }

    private void checkHealth(){        
        for(String eachName : optionMap.keySet()){
            if(usage.isRequired(eachName)){
                final Field field = optionMap.get(eachName);
                field.setAccessible(true);
                try {
                    allSucess.compareAndSet(true, field.get(optionSource) != null);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * retrieve field given its name.
     * @param name
     *      name of field
     * @return
     *      field.
     */
    private Field fieldForArg(String name) {
        final Field field = optionMap.get(name);
        ERROR_CHECKING.evaluateNullField(name, field);
        return field;
    }

    /**
     * get the appropriate option handler.
     * @param type
     *      requesting type
     * @return
     *      an available option handler for the given
     *      requesting type.
     */
    OptionHandler getOptionHandler(Type type) {
        if (type instanceof ParameterizedType) {
            final ParameterizedType parameterizedType = (ParameterizedType) type;
            final Class rawClass = (Class<?>) parameterizedType.getRawType();

            ERROR_CHECKING.evaluateCollectionParameterizedType(rawClass, type);
            final Type actualType = parameterizedType.getActualTypeArguments()[0];
            ERROR_CHECKING.evaluateNestedParameterizedType(actualType, type);

            return getOptionHandler(actualType);
        }

        if (type instanceof Class) {
            final Class<?> classType = (Class) type;

            ERROR_CHECKING.evaluateNonParameterizedCollection(classType, type);

            if (classType.isEnum()) {
                return new EnumHandler(classType);
            }

            return HANDLERS.get(classType);
        }
        throw new RuntimeException(ERROR_CHECKING.getUnsupportedFieldTypeMessage(type));
    }


    /**
     * grab the next arg value.
     * @param args
     *      arguments to inspect.
     * @param name
     *      field name
     * @param field
     *      field object
     * @return the next element of 'args' if there is one. Uses 'name' and 'field' to
     *         construct a helpful error message.
     */
    private String grabNextValue(Iterator<String> args, String name, Field field) {
        ERROR_CHECKING.evaluateNextArgAvailability(args, field, name);
        return args.next();
    }

    /**
     * Cache the available options and report any problems with the options themselves right away.
     * @return a map with all available options.
     */
    private Map<String, Field> makeOptionMap() {
        final HashMap<String, Field>    optionMap   = new HashMap<String, Field>();
        final Class<?>                  optionClass = optionSource.getClass();

        for (Field eachField : optionClass.getDeclaredFields()) {
            if (eachField.isAnnotationPresent(Option.class)) {
                final Option    option      = eachField.getAnnotation(Option.class);
                final String[]  values      = option.values();
                final boolean   required    = option.required();
                final boolean   savedInTag  = option.savedInTag();

                ERROR_CHECKING.evaluateValuesLength(values);

                // highlight only the required options if they are missing.
                if(required){
                    usage.highlight(values);
                }

                if(eachField.isAnnotationPresent(Describe.class)){
                    final Describe describe = eachField.getAnnotation(Describe.class);
                    final String description = describe.value();
                    usage.describe(values, description);
                }

                for (String eachValue : values) {
                    final Field insertedValue = optionMap.put(eachValue, eachField);
                    ERROR_CHECKING.evaluateNameUniqueness(eachValue, insertedValue);
                    if (!savedInTag) {
                        final int numArgs = getOptionHandler(eachField.getGenericType()).isBoolean() ? 0 : 1;
                        addUnsavedOption(eachValue, numArgs);
                    }
                }

                ERROR_CHECKING.evaluateOptionTypeSupport(eachField, getOptionHandler(eachField.getGenericType()));
            }
        }
        return optionMap;
    }

    @Override
    public List<String> parse(String... args) {
        final List<String> cmdArgs = Arrays.asList(args);
        if(cmdArgs.isEmpty() | cmdArgs.contains(null)) throw new IllegalArgumentException("missing args");
        return parseOptions(cmdArgs.iterator());
    }

    @Override
    public void printUsage() {
        reset();
        usage.print();
    }

    /**
     * parse any available options given by a user.
     * @param args
     *      options
     * @return
     *      a list of the positional arguments (targets) left over after processing all options.
     */
    private List<String> parseOptions(Iterator<String> args) {
        final List<String> leftovers = new ArrayList<String>();

        // Scan 'args'.
        while (args.hasNext()) {
            final String arg = args.next();
            if (arg.equals(DOUBLE_MARKS)) {
                // "--" marks the end of options and the beginning of positional arguments.
                break;
            } else if (arg.startsWith(DOUBLE_MARKS)) {
                // A long option.
                parseLongOption(arg, args);
            } else if (arg.startsWith(SINGLE_MARK)) {
                // A short option.
                parseGroupedShortOptions(arg, args);
            } else {
                // The first non-option marks the end of options.
                leftovers.add(arg);
                break;
            }
        }

        // Package up the leftovers.
        while (args.hasNext()) {
            leftovers.add(args.next());
        }

        checkHealth();
        return leftovers;
    }

    
    private void parseLongOption(String arg, Iterator<String> args) {
        String name = arg.replaceFirst("^--no-", DOUBLE_MARKS);
        String value = null;

        // Support "--name=value" as well as "--name value".
        final int equalsIndex = name.indexOf(EQUAL);
        if (equalsIndex != -1) {
            value = name.substring(equalsIndex + 1);
            name = name.substring(0, equalsIndex);
        }

        final Field field = fieldForArg(name);
        final OptionHandler handler = getOptionHandler(field.getGenericType());
        if (value == null) {
            if (handler.isBoolean()) {
                value = arg.startsWith("--no-") ? FALSE : TRUE;
            } else {
                value = grabNextValue(args, name, field);
            }
        }
        setValue(field, arg, handler, value);
    }

    /**
     * Given boolean options a and b, and non-boolean option f, we want to allow:
     * -ab
     * -abf out.txt
     * -abfout.txt
     *  (But not -abf=out.txt --- POSIX doesn't mention that either way, but GNU expressly forbids it.)
     * @param arg
     *      string of appended options
     * @param args
     *      rest of the above appended options.
     */
    private void parseGroupedShortOptions(String arg, Iterator<String> args) {
        for (int i = 1; i < arg.length(); ++i) {
            final String name = SINGLE_MARK + arg.charAt(i);
            final Field field = fieldForArg(name);
            final OptionHandler handler = getOptionHandler(field.getGenericType());
            String value;
            if (handler.isBoolean()) {
                value = TRUE;
            } else {
                // We need a value. If there's anything left, we take the rest of this "short option".
                if (i + 1 < arg.length()) {
                    value = arg.substring(i + 1);
                    i = arg.length() - 1;
                } else {
                    value = grabNextValue(args, name, field);
                }
            }

            setValue(field, arg, handler, value);
        }
    }

    /**
     * resets optionSource's fields to their defaults.
     */
    public void reset() {
        for (Map.Entry<Field, Object> entry : defaultOptionMap.entrySet()) {
            try {
                entry.getKey().set(optionSource, entry.getValue());
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        usage.reset();
    }


    public String[] readFile(File configFile) {
        if (!configFile.exists()) {
            return new String[0];
        }

        List<String> configFileLines;
        try {
            configFileLines = StringsUtil.readFileLines(configFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<String> argsList = new ArrayList<String>();
        for (String rawLine : configFileLines) {
            String line = rawLine.trim();

            // allow comments and blank lines
            if (line.startsWith(HASH) || line.isEmpty()) {
                continue;
            }
            int space = line.indexOf(SPACE);
            if (space == -1) {
                argsList.add(line);
            } else {
                argsList.add(line.substring(0, space));
                argsList.add(line.substring(space + 1).trim());
            }
        }

        return argsList.toArray(new String[argsList.size()]);
    }

    @SuppressWarnings("unchecked")
    private void setValue(Field field, String arg, OptionHandler handler, String valueText) {
        Object value = handler.translate(valueText);
        ERROR_CHECKING.evaluateTypeMismatch(value, valueText, arg, field);

        try {
            field.setAccessible(true);
            // record the original value of the field so it can be reset
            if (!defaultOptionMap.containsKey(field)) {
                defaultOptionMap.put(field, field.get(optionSource));
            }
            if (Collection.class.isAssignableFrom(field.getType())) {
                Collection collection = (Collection) field.get(optionSource);
                collection.add(value);
            } else {
                field.set(optionSource, value);
            }
        } catch (IllegalAccessException ex) {
            ERROR_CHECKING.throwRuntimeException(ex);
        }
    }

    
    /**
     * An {@link Option option}'s handler.
     */
    static abstract class OptionHandler {
        /**
         * @return {@code false} by default. Only a boolean handler
         *      should ever override this value.
         */
        boolean isBoolean() {
            return false;
        }

        /**
         * translate the string to a given type.
         * @param valueText
         *      text to be translated.
         * @return an object of appropriate type for a given {@link OptionHandler},
         *      corresponding to "valueText"
         */
        abstract Object translate(String valueText);
    }

    static class BooleanHandler extends OptionHandler {
        @Override boolean isBoolean() {
            return true;
        }

        Object translate(String valueText) {
            if (valueText.equalsIgnoreCase(TRUE) || valueText.equalsIgnoreCase(YES)) {
                return Boolean.TRUE;
            } else if (valueText.equalsIgnoreCase(FALSE) || valueText.equalsIgnoreCase(NO)) {
                return Boolean.FALSE;
            }
            return null;
        }
    }

    static class ByteHandler extends OptionHandler {
        Object translate(String valueText) {
            try {
                return Byte.parseByte(valueText);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }

    static class ShortHandler extends OptionHandler {
        Object translate(String valueText) {
            try {
                return Short.parseShort(valueText);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }

    static class IntegerHandler extends OptionHandler {
        Object translate(String valueText) {
            try {
                return Integer.parseInt(valueText);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }

    static class LongHandler extends OptionHandler {
        Object translate(String valueText) {
            try {
                return Long.parseLong(valueText);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }

    static class FloatHandler extends OptionHandler {
        Object translate(String valueText) {
            try {
                return Float.parseFloat(valueText);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }

    static class DoubleHandler extends OptionHandler {
        Object translate(String valueText) {
            try {
                return Double.parseDouble(valueText);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }

    static class StringHandler extends OptionHandler {
        Object translate(String valueText) {
            return valueText;
        }
    }

    @SuppressWarnings("unchecked") // creating an instance with a non-enum type is an error!
    static class EnumHandler extends OptionHandler {
        private final Class<?> enumType;

        public EnumHandler(Class<?> enumType) {
            this.enumType = enumType;
        }

        Object translate(String valueText) {
            try {
                return Enum.valueOf((Class) enumType, valueText.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    static class FileHandler extends OptionHandler {
        Object translate(String valueText) {
            return new File(valueText).getAbsoluteFile();
        }
    }

}
