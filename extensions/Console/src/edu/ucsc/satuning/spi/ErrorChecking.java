/****************************************************************************
 * Copyright 2010 Huascar A. Sanchez                                        *
 *                                                                          *
 * Licensed under the Apache License, Version 2.0 (the "License");          *
 * you may not use this file except in compliance with the License.         *
 * You may obtain a copy of the License at                                  *
 *                                                                          *
 *     http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                          *
 * Unless required by applicable law or agreed to in writing, software      *
 * distributed under the License is distributed on an "AS IS" BASIS,        *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 * See the License for the specific language governing permissions and      *
 * limitations under the License.                                           *
 ****************************************************************************/
package edu.ucsc.satuning.spi;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author huascar.sanchez@gmail.com (Huascar A. Sanchez)
 */
public class ErrorChecking {
    private final ErrorMessages errorMessages;

    public ErrorChecking(ErrorMessages errorMessages){
        this.errorMessages = errorMessages;
    }

    public ErrorChecking(){
        this(ErrorMessages.getInstance());
    }

    public void throwRuntimeException(Throwable cause){
        throw new RuntimeException(getInternalErrorMessage(), cause);
    }

    public ErrorMessages getErrorMessages(){
        return errorMessages;
    }

    public String getUnsupportedFieldTypeMessage(Type type){
        return getErrorMessages().unsupportedFieldType(type);
    }

    public String getInternalErrorMessage(){
        return getErrorMessages().internalError();
    }

    public void evaluateNullField(String name, Field field){
        new NullField(name, field, getErrorMessages()).evaluate();
    }

    public void evaluateCollectionParameterizedType(Class rawClass, Type type){
        new CollectionParameterizedTypeSupport(rawClass, type, getErrorMessages()).evaluate();
    }

    public void evaluateNonParameterizedCollection(Class<?> classType, Type type){
        new NonParameterizedCollectionSupport(classType, type, getErrorMessages()).evaluate();
    }

    public void evaluateNestedParameterizedType(Type actualType, Type type){
        new NestedParameterizedTypeSupport(actualType, type, getErrorMessages()).evaluate();
    }

    public void evaluateValuesLength(String[] values){
        new ValuesLength(values, getErrorMessages()).evaluate();
    }

    public void evaluateOptionTypeSupport(Field eachField, Object optionHandler){
        new OptionTypeSupport(eachField, optionHandler, getErrorMessages()).evaluate();
    }

    public void evaluateNameUniqueness(String eachValue, Field eachField){
        new NameUniqueness(eachValue, eachField, getErrorMessages()).evaluate();
    }

    public void evaluateTypeMismatch(Object value, String valueText, String arg, Field field){
        new TypeMismatch(
                value,
                valueText,
                arg,
                field,
                getErrorMessages()
        ).evaluate();
    }

    public void evaluateNextArgAvailability(Iterator<String> args, Field field, String name) {
        new NextArgAvailability(args, field, name, errorMessages).evaluate();
    }


    static class NullField implements Contract {
        ErrorMessages errorMessages;
        private final String fieldName;
        private final Field field;

        NullField(String fieldName, Field field, ErrorMessages errorMessages){
            this.fieldName      = fieldName;
            this.field          = field;
            this.errorMessages  = errorMessages;
        }

        @Override
        public void evaluate() {
            if (field == null) {
                throw new RuntimeException(errorMessages.unrecognizedOptionName(fieldName));
            }
        }
    }

    static class CollectionParameterizedTypeSupport implements Contract {
        private final Class rawType;
        private final Type type;
        private final ErrorMessages errorMessages;

        CollectionParameterizedTypeSupport(Class rawType, Type type, ErrorMessages errorMessages){
            this.rawType = rawType;
            this.type = type;
            this.errorMessages = errorMessages;
        }

        @Override
        public void evaluate() {
            if (!Collection.class.isAssignableFrom(rawType)) {
                throw new RuntimeException(errorMessages.unsupportedNonCollectionParameterizedType(type));
            }
        }
    }


    static class NonParameterizedCollectionSupport implements Contract {
        private final Class<?> classType;
        private final Type type;
        private final ErrorMessages errorMessages;

        NonParameterizedCollectionSupport(Class<?> classType, Type type, ErrorMessages errorMessages){
            this.classType = classType;
            this.type = type;
            this.errorMessages = errorMessages;
        }

        @Override
        public void evaluate() {
            if (Collection.class.isAssignableFrom(classType)) {
                // could handle by just having a default of treating
                // contents as String but consciously decided this
                // should be an error
                throw new RuntimeException(errorMessages.unsupportedNonParameterizedCollection(type));
            }
        }
    }

    static class NestedParameterizedTypeSupport implements Contract {
        private final Type actualType;
        private final Type type;
        private final ErrorMessages errorMessages;

        NestedParameterizedTypeSupport(Type actualType, Type type, ErrorMessages errorMessages){
            this.actualType     = actualType;
            this.type           = type;
            this.errorMessages  = errorMessages;
        }

        @Override
        public void evaluate() {
            if (!(actualType instanceof Class)) {
                throw new RuntimeException(errorMessages.unsupportedNestedParameterizedType(type));
            }
        }
    }

    static class ValuesLength implements Contract {
        private final String[]      values;
        private final ErrorMessages errorMessages;

        ValuesLength(String[] values, ErrorMessages errorMessages){
            this.values = values;
            this.errorMessages = errorMessages;
        }

        @Override
        public void evaluate() {
            if (values.length == 0) {
                throw new RuntimeException(errorMessages.unidentifiedOption());
            }
        }
    }

    static class OptionTypeSupport implements Contract {
        private final Field         eachField;
        private final Object        optionHandler;
        private final ErrorMessages errorMessages;

        OptionTypeSupport(Field eachField, Object optionHandler, ErrorMessages errorMessages){
            this.eachField      = eachField;
            this.optionHandler  = optionHandler;
            this.errorMessages  = errorMessages;
        }

        @Override
        public void evaluate() {
            if (optionHandler == null) {
                throw new RuntimeException(errorMessages.unsupportedOptionType(eachField.getType()));
            }
        }
    }


    static class NameUniqueness implements Contract {
        private final String                    eachValue;
        private final Field                     eachField;
        private final ErrorMessages             errorMessages;

        NameUniqueness(String eachValue, Field eachField, ErrorMessages errorMessages){
            this.eachValue      = eachValue;
            this.eachField      = eachField;
            this.errorMessages  = errorMessages;
        }

        @Override
        public void evaluate() {
            if (eachField != null) {
                throw new RuntimeException(errorMessages.multipleOptionsSameName(eachValue));
            }
        }
    }

    static class TypeMismatch implements Contract {
        private final Object        value;
        private final String        valueText;
        private final String        arg;
        private final Field field;
        private final ErrorMessages errorMessages;

        TypeMismatch(Object value, String valueText, String arg, Field field, ErrorMessages errorMessages){
            this.value = value;
            this.valueText      = valueText;
            this.arg            = arg;
            this.field          = field;
            this.errorMessages  = errorMessages;
        }

        @Override
        public void evaluate() {
            if (value == null) {
                final String type = field.getType().getSimpleName().toLowerCase();
                throw new RuntimeException(errorMessages.typeMismatchError(valueText, type, arg));
            }
        }
    }

    static class NextArgAvailability implements Contract {
        private final Iterator<String> args;
        private final Field field;
        private final String name;
        private final ErrorMessages errorMessages;

        public NextArgAvailability(Iterator<String> args, Field field, String name, ErrorMessages errorMessages) {
            this.args = args;
            this.field = field;
            this.name = name;
            this.errorMessages = errorMessages;
        }

        @Override
        public void evaluate() {
            if (!args.hasNext()) {
                final String type = field.getType().getSimpleName().toLowerCase();
                throw new RuntimeException(errorMessages.untypedOption(name, type));
            }
        }
    }


}
