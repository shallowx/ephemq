package org.meteor.support;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;

public class SerializeFeatureSupportTest {

    @Test
    public void testSerialize() throws Exception {
        TestFeatureMapperObject o = new TestFeatureMapperObject(1, "localhost:8080");
        byte[] bytes = SerializeFeatureSupport.serialize(o);

        TestFeatureMapperObject obj = SerializeFeatureSupport.deserialize(bytes, TestFeatureMapperObject.class);
        Assert.assertEquals(o, obj);
        Assert.assertEquals(o.id, obj.id);
        Assert.assertEquals(o.addr, obj.addr);
    }

    @Test
    public void testTestDeserialize() throws Exception {
        byte[] data = "{\"addr\":\"localhost:8080\",\"id\":1}".getBytes();
        TypeReference<TestFeatureMapperObject> typeRef = new TypeReference<>() {
        };
        TestFeatureMapperObject o = SerializeFeatureSupport.deserialize(data, typeRef);
        Assert.assertEquals("localhost:8080", o.addr);
        Assert.assertEquals(1, o.id);
    }

    static class TestFeatureMapperObject {
        private int id;
        private String addr;

        public TestFeatureMapperObject() {
        }

        public TestFeatureMapperObject(int id, String addr) {
            this.id = id;
            this.addr = addr;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getAddr() {
            return addr;
        }

        public void setAddr(String addr) {
            this.addr = addr;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestFeatureMapperObject that = (TestFeatureMapperObject) o;
            return id == that.id && Objects.equals(addr, that.addr);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, addr);
        }

        @Override
        public String toString() {
            return STR."{id=\{id}, addr='\{addr}'}";
        }
    }
}
