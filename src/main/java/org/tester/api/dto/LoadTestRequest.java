package org.tester.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.tester.executor.ConnectionMode;
import org.tester.model.TestPlan;
import org.tester.selector.LoadInputMode;

import java.util.Map;

/**
 * API load test configuration. The complete persona definition must be supplied inline
 * via {@link #persona}; no file paths or filesystem references are accepted.
 */
public class LoadTestRequest {

    private TestPlan persona;

    @JsonProperty("loadMode")
    @JsonAlias("loadInputMode")
    private LoadInputMode loadInputMode;

    private Map<String, Integer> valuesPerPersona;
    private int durationSeconds;
    private int rampUpSeconds;
    private int targetTps;
    private Boolean generateRequestLog;
    private Boolean trackSentRps;
    private ConnectionMode connectionMode;
    private Boolean liveReportingEnabled;
    private String stepReportFile;
    private String requestLogFile;

    public TestPlan getPersona() {
        return persona;
    }

    public void setPersona(TestPlan persona) {
        this.persona = persona;
    }

    public LoadInputMode getLoadInputMode() {
        return loadInputMode;
    }

    public void setLoadInputMode(LoadInputMode loadInputMode) {
        this.loadInputMode = loadInputMode;
    }

    public Map<String, Integer> getValuesPerPersona() {
        return valuesPerPersona;
    }

    public void setValuesPerPersona(Map<String, Integer> valuesPerPersona) {
        this.valuesPerPersona = valuesPerPersona;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(int durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public int getRampUpSeconds() {
        return rampUpSeconds;
    }

    public void setRampUpSeconds(int rampUpSeconds) {
        this.rampUpSeconds = rampUpSeconds;
    }

    public int getTargetTps() {
        return targetTps;
    }

    public void setTargetTps(int targetTps) {
        this.targetTps = targetTps;
    }

    public Boolean getGenerateRequestLog() {
        return generateRequestLog;
    }

    public void setGenerateRequestLog(Boolean generateRequestLog) {
        this.generateRequestLog = generateRequestLog;
    }

    public Boolean getTrackSentRps() {
        return trackSentRps;
    }

    public void setTrackSentRps(Boolean trackSentRps) {
        this.trackSentRps = trackSentRps;
    }

    public ConnectionMode getConnectionMode() {
        return connectionMode;
    }

    public void setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
    }

    public Boolean getLiveReportingEnabled() {
        return liveReportingEnabled;
    }

    public void setLiveReportingEnabled(Boolean liveReportingEnabled) {
        this.liveReportingEnabled = liveReportingEnabled;
    }

    public String getStepReportFile() {
        return stepReportFile;
    }

    public void setStepReportFile(String stepReportFile) {
        this.stepReportFile = stepReportFile;
    }

    public String getRequestLogFile() {
        return requestLogFile;
    }

    public void setRequestLogFile(String requestLogFile) {
        this.requestLogFile = requestLogFile;
    }
}
