package edu.ucsc.satuning.spi;

import java.lang.reflect.Type;

/**
 * A lazy-loaded singleton for error messages templates
 *
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class ErrorMessages {
    static final String UNTYPED_OPTION                  = "option %s requires a %s argument";
    static final String UNIDENTIFIED_OPTION             = "found an @Option with no name!";
    static final String MULTIPLE_OPTIONS_SHARING_NAME   = "found multiple @Options sharing the name %s.";
    static final String UNSUPPORTED_OPTION_TYPE         = "unsupported @Option field type %s";
    static final String INTERNAL_ERROR                  = "Oops! An internal error has occurred.";
    static final String UNRECOGNIZED_OPTION_NAME        = "unrecognized option %s";

    static final String UNSUPPORTED_NON_COLLECTION_PARAMETERIZED_TYPE   = "cannot handle " +
            "non-collection parameterized type %s";
    static final String UNSUPPORTED_NESTED_PARAMETERIZED_TYPE           = "cannot handle " +
            "nested parameterized type %s";
    static final String UNSUPPORTED_NON_PARAMETERIZED_COLLECTION        = "cannot handle " +
            "non-parameterized collection %s. Use a generic Collection to specify a desired element type.";

    static final String UNSUPPORTED_FIELD_TYPE  = "cannot handle unknown field type %s";
    static final String TYPE_MISMATCH_ERROR     = "couldn't convert %s to a %s for option %s";

    /**
     * private construction of an Error Messages object.
     */
    private ErrorMessages(){}

    /**
     * returns a lazy loaded singleton of error messages.
     * @return
     *      a new {@link ErrorMessages error message object}
     */
    public static ErrorMessages getInstance(){
        return Installer.INSTANCE;
    }

    public String untypedOption(String name, String type){
        return String.format(UNTYPED_OPTION, name, type);
    }

    public String unidentifiedOption(){
        return UNIDENTIFIED_OPTION;
    }

    public String multipleOptionsSameName(String name){
        return String.format(MULTIPLE_OPTIONS_SHARING_NAME, name);
    }

    public String unsupportedOptionType(Class<?> type) {
        return String.format(UNSUPPORTED_OPTION_TYPE, type);
    }

    public String internalError(){
        return INTERNAL_ERROR;
    }

    public String unsupportedNonCollectionParameterizedType(Type type){
        return String.format(UNSUPPORTED_NON_COLLECTION_PARAMETERIZED_TYPE, type);
    }

    public String unsupportedNestedParameterizedType(Type type){
        return String.format(UNSUPPORTED_NESTED_PARAMETERIZED_TYPE, type);
    }

    public String unsupportedNonParameterizedCollection(Type type){
        return String.format(UNSUPPORTED_NON_PARAMETERIZED_COLLECTION, type);
    }

    public String unsupportedFieldType(Type type){
        return String.format(UNSUPPORTED_FIELD_TYPE, type);
    }

    public String unrecognizedOptionName(String name){
        return String.format(UNRECOGNIZED_OPTION_NAME, name);
    }

    public String typeMismatchError(String valueText, String type, String arg){
        return String.format(TYPE_MISMATCH_ERROR, valueText, type, arg);
    }

    /**
     * lazy installer.
     */
    private static class Installer {
        private static final ErrorMessages INSTANCE = new ErrorMessages();
        private Installer(){}
    }
}
