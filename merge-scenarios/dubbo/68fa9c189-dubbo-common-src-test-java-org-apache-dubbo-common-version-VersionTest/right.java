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

    @Test
    public void testGetIntVersion() {
        Assert.assertEquals(2060100, Version.getIntVersion("2.6.1"));
        Assert.assertEquals(2060101, Version.getIntVersion("2.6.1.1"));
        Assert.assertEquals(2070001, Version.getIntVersion("2.7.0.1"));
        Assert.assertEquals(2070000, Version.getIntVersion("2.7.0"));
    }

    @Test
    public void testIsFramework270OrHigher() {
        Assert.assertTrue(Version.isRelease270OrHigher("2.7.0"));
        Assert.assertTrue(Version.isRelease270OrHigher("2.7.0.1"));
        Assert.assertTrue(Version.isRelease270OrHigher("2.7.0.2"));
        Assert.assertTrue(Version.isRelease270OrHigher("2.8.0"));
        Assert.assertFalse(Version.isRelease270OrHigher("2.6.3"));
        Assert.assertFalse(Version.isRelease270OrHigher("2.6.3.1"));
    }

    @Test
    public void testIsFramework263OrHigher() {
        Assert.assertTrue(Version.isRelease263OrHigher("2.7.0"));
        Assert.assertTrue(Version.isRelease263OrHigher("2.7.0.1"));
        Assert.assertTrue(Version.isRelease263OrHigher("2.6.4"));
        Assert.assertFalse(Version.isRelease263OrHigher("2.6.2"));
        Assert.assertFalse(Version.isRelease263OrHigher("2.6.1.1"));
        Assert.assertTrue(Version.isRelease263OrHigher("2.6.3"));
    }
}