package com.datastax.example;

import java.io.Serializable;

/**
 * Created by davidfelcey on 24/05/2016.
 */
public class Detail implements Serializable {
    private String docId;
    private String detailId;
    private String name;
    private String description;

    public Detail() {
    }

    public Detail(String docId, String detailId, String name, String description) {
        this.docId = docId;
        this.detailId = detailId;
        this.name = name;
        this.description = description;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getDetailId() {
        return detailId;
    }

    public void setDetailId(String detailId) {
        this.detailId = detailId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
