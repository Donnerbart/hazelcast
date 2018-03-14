/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.merge;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Before merging, put some entries to the merging sub-cluster and expect not to see them after merge.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class IgnoreMergingEntriesMapSplitBrainTest extends SplitBrainTestSupport {

    private final String testMapName = randomMapName();

    private MergeLifecycleListener mergeLifecycleListener;

    @Override
    protected Config config() {
        MergePolicyConfig mergePolicyConfig = new MergePolicyConfig()
                .setPolicy(IgnoreMergingEntryMapMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig(testMapName)
                .setMergePolicyConfig(mergePolicyConfig);

        return super.config()
                .addMapConfig(mapConfig);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        IMap<Integer, Integer> mapOnFirstBrain = secondBrain[0].getMap(testMapName);
        for (int i = 0; i < 100; i++) {
            mapOnFirstBrain.put(i, i);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        IMap<Integer, Integer> map = instances[0].getMap(testMapName);
        assertTrue(map.isEmpty());
    }
}
