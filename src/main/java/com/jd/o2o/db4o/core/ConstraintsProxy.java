package com.jd.o2o.db4o.core;

import com.db4o.query.Constraint;
import com.db4o.query.Constraints;

/**
 * Constraints代理类
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月27日 下午7:33:52
 */
public class ConstraintsProxy extends ConstraintProxy implements Constraints {
  private final Constraints constraint;

  public ConstraintsProxy(Constraints constraint, QueryProxy queryProxy) {
    super(constraint, queryProxy);
    this.constraint = constraint;
  }

  @Override
  public Constraint[] toArray() {
    return constraint.toArray();
  }
}
