package org.example.config;

/**
 * TODO
 *
 * @author lile
 * @description
 **/
public class CliOptions {
    private final String sqlFilePath;
    private final String workingSpace;

    public CliOptions(String sqlFilePath, String workingSpace) {
        this.sqlFilePath = sqlFilePath;
        this.workingSpace = workingSpace;
        //this.hiveConfPath = hiveConfPath;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public String getWorkingSpace() {
        return workingSpace;
    }
}
