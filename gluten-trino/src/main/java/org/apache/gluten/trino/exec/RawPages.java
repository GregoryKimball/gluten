/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.trino.exec;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;

/**
 * Converts between Trino pages and the layout exchanged with libgluten_trino.so: an int32
 * position count, an int32 block count, then each block as written by {@link
 * BlockEncodingSerde#writeBlock}. Native code reads and writes the same layout in {@code
 * TrinoPageSerde}.
 */
public final class RawPages {
  private RawPages() {}

  public static byte[] toBytes(BlockEncodingSerde serde, Page page) {
    DynamicSliceOutput output = new DynamicSliceOutput(Math.toIntExact(page.getSizeInBytes() + 64));
    output.writeInt(page.getPositionCount());
    output.writeInt(page.getChannelCount());
    for (int channel = 0; channel < page.getChannelCount(); channel++) {
      serde.writeBlock(output, page.getBlock(channel));
    }
    return output.slice().getBytes();
  }

  public static Page toPage(BlockEncodingSerde serde, byte[] bytes) {
    Slice slice = Slices.wrappedBuffer(bytes);
    SliceInput input = slice.getInput();
    int positionCount = input.readInt();
    int blockCount = input.readInt();
    Block[] blocks = new Block[blockCount];
    for (int i = 0; i < blockCount; i++) {
      blocks[i] = serde.readBlock(input);
    }
    return new Page(positionCount, blocks);
  }
}
