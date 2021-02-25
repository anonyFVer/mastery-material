package org.apache.dubbo.common.version;

import org.apache.dubbo.common.Version;
import org.junit.Assert;
import org.junit.Test;

public class VersionTest {

    @Test
    public void testGetProtocolVersion() {
        Assert.assertEquals(Version.getProtocolVersion(), Version.DEFAULT_DUBBO_PROTOCOL_VERSION);
    }

    @Test
    public void testSupportResponseAttachment() {
        Assert.assertTrue(Version.isSupportResponseAttachment("2.0.2"));
        Assert.assertTrue(Version.isSupportResponseAttachment("2.0.3"));
        Assert.assertFalse(Version.isSupportResponseAttachment("2.0.0"));
    }
}