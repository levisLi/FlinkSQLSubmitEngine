package org.example.config;

import org.apache.commons.lang3.StringUtils;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class CliOptions {
    private final String sqlFilePath;
    private final String workingSpace;
    private String deployMode="cluster";

    public CliOptions(String sqlFilePath, String workingSpace,String deployMode) {
        this.sqlFilePath = sqlFilePath;
        this.workingSpace = workingSpace;
        if(StringUtils.isNotBlank(deployMode)){
            this.deployMode=deployMode;
        }
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public String getWorkingSpace() {
        return workingSpace;
    }
}
