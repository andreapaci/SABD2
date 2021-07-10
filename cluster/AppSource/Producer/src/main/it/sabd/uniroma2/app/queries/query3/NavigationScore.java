package it.sabd.uniroma2.app.queries.query3;

public class NavigationScore {
    String ts;
    String tripId;
    String navigaionScore;

    public NavigationScore(){ }

    public NavigationScore(String ts, String tripId, String navigationScore){
        this.ts = ts;
        this.tripId = tripId;
        this.navigaionScore = navigationScore;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getNavigaionScore() {
        return navigaionScore;
    }

    public void setNavigaionScore(String navigaionScore) {
        this.navigaionScore = navigaionScore;
    }

    @Override
    public String toString() {
        return "NavigationScore{" +
                "ts='" + ts + '\'' +
                ", tripId='" + tripId + '\'' +
                ", navigaionScore='" + navigaionScore + '\'' +
                '}';
    }
}
