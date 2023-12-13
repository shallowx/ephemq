package org.meteor.coordinatio;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

public class JsonCoordinatorTests {

    @Test
    public void testSerialize() throws Exception {
        JsonObjectTest o = new JsonObjectTest(1, "localhost:8080");
        byte[] bytes = JsonCoordinator.serialize(o);

        JsonObjectTest obj = JsonCoordinator.deserialize(bytes, JsonObjectTest.class);
        Assert.assertEquals(o, obj);
        Assert.assertEquals(o.id, obj.id);
        Assert.assertEquals(o.addr, obj.addr);
    }

    @Test
    public void testTestDeserialize() throws Exception {
        byte[] data = "{\"addr\":\"localhost:8080\",\"id\":1}".getBytes();
        TypeReference<JsonObjectTest> typeRef = new TypeReference<>() {
        };
        JsonObjectTest o = JsonCoordinator.deserialize(data, typeRef);
        Assert.assertEquals("localhost:8080", o.addr);
        Assert.assertEquals(1, o.id);
    }

    static class JsonObjectTest {
        private int id;
        private String addr;

        public JsonObjectTest() {
        }

        public JsonObjectTest(int id, String addr) {
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
            JsonObjectTest that = (JsonObjectTest) o;
            return id == that.id && Objects.equals(addr, that.addr);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, addr);
        }

        @Override
        public String toString() {
            return "{" +
                    "id=" + id +
                    ", addr='" + addr + '\'' +
                    '}';
        }
    }
}
