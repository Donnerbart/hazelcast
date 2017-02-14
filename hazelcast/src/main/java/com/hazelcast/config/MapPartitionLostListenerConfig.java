/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.config;

import com.hazelcast.map.listener.MapPartitionLostListener;

import java.util.EventListener;

/**
 * Configuration for MapPartitionLostListener
 *
 * @see com.hazelcast.map.listener.MapPartitionLostListener
 */
public class MapPartitionLostListenerConfig extends ListenerConfig {

    private MapPartitionLostListenerConfigReadOnly readOnly;

    public MapPartitionLostListenerConfig() {
    }

    public MapPartitionLostListenerConfig(String className) {
        super(className);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListener implementation) {
        super(implementation);
    }

    public MapPartitionLostListenerConfig(MapPartitionLostListenerConfig config) {
        implementation = config.getImplementation();
        className = config.getClassName();
    }

    @Override
    public MapPartitionLostListener getImplementation() {
        return (MapPartitionLostListener) implementation;
    }

    public MapPartitionLostListenerConfig setImplementation(final MapPartitionLostListener implementation) {
        super.setImplementation(implementation);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MapPartitionLostListenerConfig that = (MapPartitionLostListenerConfig) o;

        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        return !(implementation != null ? !implementation.equals(that.implementation) : that.implementation != null);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        return result;
    }

    @Override
    MapPartitionLostListenerConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapPartitionLostListenerConfigReadOnly(this);
        }
        return readOnly;
    }

    private static class MapPartitionLostListenerConfigReadOnly extends MapPartitionLostListenerConfig {

        MapPartitionLostListenerConfigReadOnly(MapPartitionLostListenerConfig config) {
            super(config);
        }

        @Override
        public MapPartitionLostListener getImplementation() {
            return (MapPartitionLostListener) implementation;
        }

        @Override
        public ListenerConfig setClassName(String className) {
            throw new UnsupportedOperationException("this config is read-only");
        }

        @Override
        public ListenerConfig setImplementation(EventListener implementation) {
            throw new UnsupportedOperationException("this config is read-only");
        }

        @Override
        public MapPartitionLostListenerConfig setImplementation(MapPartitionLostListener implementation) {
            throw new UnsupportedOperationException("this config is read-only");
        }
    }
}
