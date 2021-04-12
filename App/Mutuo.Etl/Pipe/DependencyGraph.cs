using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SysExtensions.Collections;

namespace Mutuo.Etl.Pipe {
  public class DependencyGraph<T> {
    readonly MultiValueDictionary<string, string> DepsByFrom = new MultiValueDictionary<string, string>();
    readonly MultiValueDictionary<string, string> DepsByTo   = new MultiValueDictionary<string, string>();
    readonly IKeyedCollection<string, T>          _nodes;
    readonly Expression<Func<T, string>>          _getKey;

    public DependencyGraph(IEnumerable<T> nodes, Func<T, IEnumerable<string>> getDependencies, Expression<Func<T, string>> getKey) {
      _getKey = getKey;
      _nodes = new KeyedCollection<string, T>(getKey);
      _nodes.AddRange(nodes);

      foreach (var node in _nodes)
      foreach (var d in getDependencies(node))
        AddDependency(GetKey(node), d);
    }

    public T this[string key] => _nodes[key];

    string GetKey(T item) => _nodes.GetKey(item);

    public IReadOnlyCollection<T> Nodes => _nodes.ToList();

    public void AddDependency(string from, string to) {
      DepsByFrom.Add(from, to);
      DepsByTo.Add(to, from);
    }

    public IEnumerable<T> Dependencies(T node) => DepsByFrom.TryGet(GetKey(node)).Select(to => _nodes[to]).NotNull();

    public IEnumerable<T> DependenciesDeep(T node) {
      var discoveredDeps = new KeyedCollection<string, T>(_getKey);

      IEnumerable<T> InnerDescendentDeps(T n) {
        var childDeps = Dependencies(n).ToList();
        foreach (var dep in childDeps.Where(c => !discoveredDeps.Contains(c)))
          yield return discoveredDeps.AddItem(dep);

        foreach (var dep in childDeps.SelectMany(InnerDescendentDeps))
          yield return dep;
      }

      foreach (var dep in InnerDescendentDeps(node))
        yield return dep;
    }

    public IEnumerable<T> Dependants(T node) => DepsByTo.TryGet(GetKey(node)).Select(from => _nodes[from]);
  }
}