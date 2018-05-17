package org.daiv.immutable.utils.persistence.annotations;

import java.lang.annotation.Annotation;

public class FLCreator {

    public static FlatList get(String name){
        return new FlatList(){
            @Override
            public Class<? extends Annotation> annotationType() {
                return FlatList.class;
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean isTableName() {
                return false;
            }

            @Override
            public String toString() {
                return name + " - size: " + 1;
            }
        };

    }
}
