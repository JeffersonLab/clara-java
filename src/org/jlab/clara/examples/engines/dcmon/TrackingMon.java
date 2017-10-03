package org.jlab.clara.examples.engines.dcmon;


import org.jlab.io.base.DataBank;
import org.jlab.io.base.DataEvent;

/**
 *
 * @author ziegler
 */
public class TrackingMon {

    public TrackingMon() {
        _TimeResidual = new float[3]; //per region
    }

    public int getNbHBTracks() {
        return _NbHBTracks;
    }

    public void setNbHBTracks(int _NbHBTracks) {
        this._NbHBTracks = _NbHBTracks;
    }

    public int getNbTBTracks() {
        return _NbTBTracks;
    }

    public void setNbTBTracks(int _NbTBTracks) {
        this._NbTBTracks = _NbTBTracks;
    }

    public int getNbCTTracks() {
        return _NbCTTracks;
    }

    public void setNbCTTracks(int _NbCTTracks) {
        this._NbCTTracks = _NbCTTracks;
    }

    public int getNbHBHits() {
        return _NbHBHits;
    }

    public void setNbHBHits(int _NbHBHits) {
        this._NbHBHits = _NbHBHits;
    }

    public int getNbTBHits() {
        return _NbTBHits;
    }

    public void setNbTBHits(int _NbTBHits) {
        this._NbTBHits = _NbTBHits;
    }

    public int getNbCTHits() {
        return _NbCTHits;
    }

    public void setNbCTHits(int _NbCTHits) {
        this._NbCTHits = _NbCTHits;
    }

    public int getNbHBHitsOnTrack() {
        return _NbHBHitsOnTrack;
    }

    public void setNbHBHitsOnTrack(int _NbHBHitsOnTrack) {
        this._NbHBHitsOnTrack = _NbHBHitsOnTrack;
    }

    public int getNbTBHitsOnTrack() {
        return _NbTBHitsOnTrack;
    }

    public void setNbTBHitsOnTrack(int _NbTBHitsOnTrack) {
        this._NbTBHitsOnTrack = _NbTBHitsOnTrack;
    }

    public int getNbCTHitsOnTrack() {
        return _NbCTHitsOnTrack;
    }

    public void setNbCTHitsOnTrack(int _NbCTHitsOnTrack) {
        this._NbCTHitsOnTrack = _NbCTHitsOnTrack;
    }

    public float[] getTimeResidual() {
        return _TimeResidual;
    }

    public void setTimeResidual(float[] _TimeResidual) {
        this._TimeResidual = _TimeResidual;
    }

    private int _NbHBTracks;    //number of hit-based tracks per event
    private int _NbTBTracks;    //number of time-based tracks per event
    private int _NbCTTracks;    //number of central tracks per event
    private int _NbHBHits;      //number of hit-based hits per event
    private int _NbTBHits;      //number of time-based hits per event
    private int _NbCTHits;      //number of central hits per event
    private int _NbHBHitsOnTrack; //average number of hit-based hits on track per event
    private int _NbTBHitsOnTrack; //average number of time-based hits on track per event
    private int _NbCTHitsOnTrack; //average number of central hits on track per event
    private float[] _TimeResidual;//average time residual per region

    public void fetch_Trks(DataEvent event) {
        this.init();
        if (event.hasBank("CVTRec::Tracks")) {
            DataBank bank = event.getBank("CVTRec::Tracks");
            this._NbCTTracks= bank.rows();
        }

        if (event.hasBank("BSTRec::Hits")) {
            DataBank bank = event.getBank("BSTRec::Hits");
            this._NbCTHits+= bank.rows();
            for(int i = 0; i < bank.rows(); i++) {
                if(bank.getShort("trkID", i)>-1 && this._NbCTTracks>0)
                    this._NbCTHitsOnTrack++;
            }
        }
        if (event.hasBank("BMTRec::Hits")) {
            DataBank bank = event.getBank("BMTRec::Hits");
            this._NbCTHits+= bank.rows();
            for(int i = 0; i < bank.rows(); i++) {
                if(bank.getShort("trkID", i)>-1 && this._NbCTTracks>0)
                    this._NbCTHitsOnTrack++;
            }
        }
        if(this._NbCTHitsOnTrack>0 && this._NbCTTracks>0)
            this._NbCTHitsOnTrack/=this._NbCTTracks;

        if (event.hasBank("TimeBasedTrkg::TBHits")) {
            DataBank bank = event.getBank("TimeBasedTrkg::TBHits");
            this._NbTBHits= bank.rows();
            for(int i = 0; i < bank.rows(); i++) {
                int region = ((int)bank.getByte("superlayer", i) + 1) / 2;
                this._TimeResidual[region-1]+=bank.getFloat("timeResidual", i);
            }
            for(int r = 0; r < 3; r++)
                this._TimeResidual[r]/=bank.rows();
        }

        if (event.hasBank("HitBasedTrkg::HBHits")) {
            DataBank bank = event.getBank("HitBasedTrkg::HBHits");
            this._NbHBHits= bank.rows();
        }

        if (event.hasBank("TimeBasedTrkg::TBTracks")) {
            DataBank bank = event.getBank("TimeBasedTrkg::TBTracks");
            this._NbTBTracks= bank.rows();
            for(int i = 0; i < bank.rows(); i++) {
                this._NbTBHitsOnTrack+=bank.getShort("ndf", i)+6;//ndf+6=nb hits on track
            }
            this._NbTBHitsOnTrack/=bank.rows();
        }

        if (event.hasBank("HitBasedTrkg::HBTracks")) {
            DataBank bank = event.getBank("HitBasedTrkg::HBTracks");
            this._NbHBTracks= bank.rows();
            for(int i = 0; i < bank.rows(); i++) {
                this._NbHBHitsOnTrack+=bank.getShort("ndf", i)+6;//ndf+6=nb hits on track
            }
            this._NbHBHitsOnTrack/=bank.rows();
        }

    }


    private void init(){
        _NbHBTracks      = 0;
        _NbTBTracks      = 0;
        _NbCTTracks      = 0;
        _NbHBHits        = 0;
        _NbTBHits        = 0;
        _NbCTHits        = 0;
        _NbHBHitsOnTrack = 0;
        _NbTBHitsOnTrack = 0;
        _NbCTHitsOnTrack = 0;
        _TimeResidual[0] = 0;
        _TimeResidual[1] = 0;
        _TimeResidual[2] = 0;
    }
}
