package com.cqz.component.kudu;

public enum TableOptions {
    CREATE_TABLE(0),
    DROP_TABLE(1),
    UNKNOWN(9);

    private final Integer code;

    TableOptions(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }

    public static TableOptions getByCode(Integer code){
        for (TableOptions value : values()) {
            if (value.getCode().equals(code)){
                return value;
            }
        }
        return UNKNOWN;
    }

}
