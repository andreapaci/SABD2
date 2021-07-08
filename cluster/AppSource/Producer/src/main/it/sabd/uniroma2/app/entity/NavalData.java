package it.sabd.uniroma2.app.entity;

import it.sabd.uniroma2.app.enums.Seas;
import it.sabd.uniroma2.app.enums.TimeSlot;

import java.text.SimpleDateFormat;
import java.util.Date;

public class NavalData {

    private Date ts;
    private String id;
    private String shipType;
    //private int speed;
    private float lon;
    private float lat;
    private String cell;
    private Seas sea;
    private TimeSlot timeSlot;
    //private String course;
    //private String heading;
    //private String departurePort;
    //private String reportedDraugth;
    private String tripId;

    public NavalData(Date ts, String id, String shipType, float lon, float lat, String tripId) {
        this.ts = ts;
        this.id = id;
        this.shipType = shipType;
        this.lon = lon;
        this.lat = lat;
        this.tripId = tripId;
    }

    //TODO: da rimuovere
    public NavalData(Date ts, String id, int shipType, float lon, float lat, String tripId) {
        this.ts = ts;
        this.id = id;
        this.shipType = String.valueOf(shipType);
        this.lon = lon;
        this.lat = lat;
        this.tripId = tripId;
    }


    public String getFormattedTs() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        return format.format(ts);
    }

    public Date getTs() { return ts; }

    public String getId() { return id; }

    public String getShipType() {
        return shipType;
    }

    public float getLon() {
        return lon;
    }

    public float getLat() {
        return lat;
    }

    public String getCell() {
        return cell;
    }

    public Seas getSea() {
        return sea;
    }

    public String getStringSea() { return sea.toString(); }

    public TimeSlot getTimeSlot() {
        return timeSlot;
    }

    public String getTripId() { return tripId; }

    public void setCell(String cell) {
        this.cell = cell;
    }

    public void setSea(Seas sea) {
        this.sea = sea;
    }

    public void setTimeSlot(TimeSlot timeSlot) {
        this.timeSlot = timeSlot;
    }

    @Override
    public String toString() {
        return "NavalData{" +
                "ts=" + getFormattedTs() +
                ", id='" + id + '\'' +
                ", shipType='" + shipType + '\'' +
                ", lon=" + lon +
                ", lat=" + lat +
                ", cell='" + cell + '\'' +
                ", sea=" + sea +
                ", timeSlot=" + timeSlot +
                '}';
    }
}
