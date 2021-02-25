package org.apache.dubbo.common.serialize.protostuff;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

public class ProtostuffObjectOutputTest {

    private ByteArrayOutputStream byteArrayOutputStream;

    private ProtostuffObjectOutput protostuffObjectOutput;

    private ProtostuffObjectInput protostuffObjectInput;

    private ByteArrayInputStream byteArrayInputStream;

    @BeforeEach
    public void setUp() throws Exception {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.protostuffObjectOutput = new ProtostuffObjectOutput(byteArrayOutputStream);
    }

    @Test
    public void testWriteObjectNull() throws IOException, ClassNotFoundException {
        this.protostuffObjectOutput.writeObject(null);
        this.flushToInput();
        assertThat(protostuffObjectInput.readObject(), nullValue());
    }

    @Test
    public void testSerializeTimestamp() throws IOException, ClassNotFoundException {
        Timestamp originTime = new Timestamp(System.currentTimeMillis());
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();
        Timestamp serializedTime = protostuffObjectInput.readObject(Timestamp.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testSerializeSqlDate() throws IOException, ClassNotFoundException {
        java.sql.Date originTime = new java.sql.Date(System.currentTimeMillis());
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();
        java.sql.Date serializedTime = protostuffObjectInput.readObject(java.sql.Date.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testSerializeSqlTime() throws IOException, ClassNotFoundException {
        java.sql.Time originTime = new java.sql.Time(System.currentTimeMillis());
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();
        java.sql.Time serializedTime = protostuffObjectInput.readObject(java.sql.Time.class);
        assertThat(serializedTime, is(originTime));
    }

    @Test
    public void testSerializeDate() throws IOException, ClassNotFoundException {
        Date originTime = new Date();
        this.protostuffObjectOutput.writeObject(originTime);
        this.flushToInput();
        Date serializedTime = protostuffObjectInput.readObject(Date.class);
        assertThat(serializedTime, is(originTime));
    }

    private void flushToInput() throws IOException {
        this.protostuffObjectOutput.flushBuffer();
        this.byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        this.protostuffObjectInput = new ProtostuffObjectInput(byteArrayInputStream);
    }
}