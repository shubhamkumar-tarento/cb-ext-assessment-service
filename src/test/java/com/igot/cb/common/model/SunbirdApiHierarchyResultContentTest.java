package com.igot.cb.common.model;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SunbirdApiHierarchyResultContentTest {

    @Test
    void testGettersAndSetters() {
        SunbirdApiHierarchyResultContent content = new SunbirdApiHierarchyResultContent();

        String parent = "parentId";
        String identifier = "contentId";
        String downloadUrl = "http://example.com/download";
        String channel = "channel1";
        String source = "source1";
        String mimeType = "application/pdf";
        String objectType = "Content";
        String primaryCategory = "Learning Resource";
        String artifactUrl = "http://example.com/artifact";
        String contentType = "Resource";
        String status = "Live";
        String name = "Content Name";
        String code = "CNT123";
        String streamingUrl = "http://example.com/stream";
        int leafNodesCount = 3;

        SunbirdApiHierarchyResultContent child = new SunbirdApiHierarchyResultContent();
        child.setIdentifier("childId");
        List<SunbirdApiHierarchyResultContent> children = Collections.singletonList(child);

        SunbirdApiBatchResp batch = new SunbirdApiBatchResp();
        batch.setBatchId("batch1");
        List<SunbirdApiBatchResp> batches = Collections.singletonList(batch);

        content.setParent(parent);
        content.setIdentifier(identifier);
        content.setDownloadUrl(downloadUrl);
        content.setChannel(channel);
        content.setSource(source);
        content.setMimeType(mimeType);
        content.setObjectType(objectType);
        content.setPrimaryCategory(primaryCategory);
        content.setArtifactUrl(artifactUrl);
        content.setContentType(contentType);
        content.setStatus(status);
        content.setName(name);
        content.setCode(code);
        content.setStreamingUrl(streamingUrl);
        content.setChildren(children);
        content.setBatches(batches);
        content.setLeafNodesCount(leafNodesCount);

        assertEquals(parent, content.getParent());
        assertEquals(identifier, content.getIdentifier());
        assertEquals(downloadUrl, content.getDownloadUrl());
        assertEquals(channel, content.getChannel());
        assertEquals(source, content.getSource());
        assertEquals(mimeType, content.getMimeType());
        assertEquals(objectType, content.getObjectType());
        assertEquals(primaryCategory, content.getPrimaryCategory());
        assertEquals(artifactUrl, content.getArtifactUrl());
        assertEquals(contentType, content.getContentType());
        assertEquals(status, content.getStatus());
        assertEquals(name, content.getName());
        assertEquals(code, content.getCode());
        assertEquals(streamingUrl, content.getStreamingUrl());
        assertEquals(children, content.getChildren());
        assertEquals(batches, content.getBatches());
        assertEquals(leafNodesCount, content.getLeafNodesCount());
    }

    @Test
    void testDefaultValues() {
        SunbirdApiHierarchyResultContent content = new SunbirdApiHierarchyResultContent();

        assertNull(content.getParent());
        assertNull(content.getIdentifier());
        assertNull(content.getDownloadUrl());
        assertNull(content.getChannel());
        assertNull(content.getSource());
        assertNull(content.getMimeType());
        assertNull(content.getObjectType());
        assertNull(content.getPrimaryCategory());
        assertNull(content.getArtifactUrl());
        assertNull(content.getContentType());
        assertNull(content.getStatus());
        assertNull(content.getName());
        assertNull(content.getCode());
        assertNull(content.getStreamingUrl());
        assertNull(content.getChildren());
        assertNull(content.getBatches());
        assertEquals(0, content.getLeafNodesCount());
    }
}
