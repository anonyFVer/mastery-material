package org.apache.dubbo.common.version;

import org.apache.dubbo.common.Version;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VersionTest {

    @Test
    public void testGetProtocolVersion() {
        Assertions.assertEquals(Version.getProtocolVersion(), Version.DEFAULT_DUBBO_PROTOCOL_VERSION);
    }

    @Test
    public void testSupportResponseAttachment() {
        Assertions.assertTrue(Version.isSupportResponseAttachment("2.0.2"));
        Assertions.assertTrue(Version.isSupportResponseAttachment("2.0.3"));
        Assertions.assertFalse(Version.isSupportResponseAttachment("2.0.0"));
    }
}