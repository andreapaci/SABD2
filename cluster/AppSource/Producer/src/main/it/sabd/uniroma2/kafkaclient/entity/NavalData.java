package it.sabd.uniroma2.kafkaclient.entity;

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
    //private String course;
    //private String heading;
    //private String departurePort;
    //private String reportedDraugth;
    //private String tripId;

    public NavalData(Date ts, String id, String shipType, float lon, float lat) {
        this.ts = ts;
        this.id = id;
        this.shipType = shipType;
        this.lon = lon;
        this.lat = lat;
    }

    public Date getTs() {
        return ts;
    }

    public String getId() {
        return id;
    }

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

    public void setCell(String cell) {
        this.cell = cell;
    }

    public void setSea(Seas sea) {
        this.sea = sea;
    }
}
