package esac.archive.ehst.dl.caom2.network.performance;

import java.util.Date;

public class NetworkPerformanceResult {
	
	private Date startDate = null;
	private Date endDate = null;
    private long numFileDownloaded = 0;
    private long elapsedTimeMs = 0;
    private long bytesDownloaded = 0;
    
	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public long getNumFileDownloaded() {
		return numFileDownloaded;
	}
	public void setNumFileDownloaded(long numFileDownloaded) {
		this.numFileDownloaded = numFileDownloaded;
	}
	public long getElapsedTimeMs() {
		return elapsedTimeMs;
	}
	public void setElapsedTimeMs(long elapsedTimeMs) {
		this.elapsedTimeMs = elapsedTimeMs;
	}
	public long getBytesDownloaded() {
		return bytesDownloaded;
	}
	public void setBytesDownloaded(long bytesDownloaded) {
		this.bytesDownloaded = bytesDownloaded;
	}

	@Override
	public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nNetwork performance between '" + startDate + "' and '" + endDate + "'");
        sb.append("\n\n\tFiles downloaded: " + numFileDownloaded);
        sb.append("\n\tElapsed time (ms): " + elapsedTimeMs);
        sb.append("\n\tBytes downloaded: " + bytesDownloaded);

        return sb.toString();
	}
}
