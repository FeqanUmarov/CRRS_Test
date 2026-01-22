function createFeatureOwnershipTracker(){
  const FeatureOwner = new WeakMap();

  function trackFeatureOwnership(source){
    if (!source) return;
    source.on('addfeature',  e => { try { FeatureOwner.set(e.feature, source); } catch {} });
    source.on('removefeature', e => { try { FeatureOwner.delete(e.feature); } catch {} });
  }

  function getOwner(feature){
    return FeatureOwner.get(feature) || null;
  }

  return {
    trackFeatureOwnership,
    getOwner
  };
}

window.FeatureOwnership = createFeatureOwnershipTracker();