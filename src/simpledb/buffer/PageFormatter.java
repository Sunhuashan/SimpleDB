package simpledb.buffer;

import simpledb.file.Page;

/**
 * 格式化物理块的接口
 *
 *
 * @author shs
 * @date 2022/5/22 17:02
 */
public interface PageFormatter {
    public void format(Page page);
}
