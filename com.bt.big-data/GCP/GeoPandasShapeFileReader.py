from functools import partial
from itertools import izip

import geopandas as gpd
import matplotlib.pyplot as plt
from geopandas import GeoDataFrame
import matplotlib.pyplot as plt
from descartes import PolygonPatch

# fp = "/home/cloudera/Documents/tesseract_1.4.7.tar/London.shp"
# data = gpd.read_file(fp)
# gridCRS = data.crs

# data['geometry'] = data['geometry'].to_crs(crs=gridCRS)
# print data,gridCRS
# print dir(gridCRS),gridCRS.viewitems(),gridCRS.keys()
# print dir(data),data.values
# my_map = data.plot(column="car_r_t", linewidth=0.03, cmap="Reds", scheme="quantiles", k=9, alpha=0.9)

# print type(data)
#
# print "H", data
# print data.plot()
# print data.crs
#
#
# grid = gpd.read_file(fp)
from mpl_toolkits.basemap.pyproj import transform, Proj


def transformToBNG(func, geom):
    """Applies `func` to all coordinates of `geom` and returns a new
    geometry of the same type from the transformed coordinates.

    `func` maps x, y, and optionally z to output xp, yp, zp. The input
   parameters may iterable types like lists or arrays or single values.
    The output shall be of the same type. Scalars in, scalars out.
    Lists in, lists out.

    For example, here is an identity function applicable to both types
    of input.

      def id_func(x, y, z=None):
          return tuple(filter(None, [x, y, z]))

      g2 = transform(id_func, g1)

    A partially applied transform function from pyproj satisfies the
    requirements for `func`.

      from functools import partial
      import pyproj

      project = partial(
          pyproj.transform,
          pyproj.Proj(init='epsg:4326'),
          pyproj.Proj(init='epsg:26913'))

      g2 = transform(project, g1)

    Lambda expressions such as the one in

      g2 = transform(lambda x, y, z=None: (x+1.0, y+1.0), g1)

    also satisfy the requirements for `func`.
    """
    # if geom.is_empty:
    #    return geom
    if geom.geom_type in ('Point', 'LineString', 'LinearRing', 'Polygon'):

        # First we try to apply func to x, y, z sequences. When func is
        # optimized for sequences, this is the fastest, though zipping
        # the results up to go back into the geometry constructors adds
        # extra cost.
        try:
            if geom.geom_type in ('Point', 'LineString', 'LinearRing'):
                return type(geom)(zip(*func(*izip(*geom.coords))))
            elif geom.geom_type == 'Polygon':
                shell = type(geom.exterior)(
                    zip(*func(*izip(*geom.exterior.coords))))
                holes = list(type(ring)(zip(*func(*izip(*ring.coords))))
                             for ring in geom.interiors)
                return type(geom)(shell, holes)

        # A func that assumes x, y, z are single values will likely raise a
        # TypeError, in which case we'll try again.
        except TypeError:
            if geom.geom_type in ('Point', 'LineString', 'LinearRing'):
                return type(geom)([func(*c) for c in geom.coords])
            elif geom.geom_type == 'Polygon':
                shell = type(geom.exterior)(
                    [func(*c) for c in geom.exterior.coords])
                holes = list(type(ring)([func(*c) for c in ring.coords])
                             for ring in geom.interiors)
                return type(geom)(shell, holes)

    elif geom.type.startswith('Multi') or geom.type == 'GeometryCollection':
        return type(geom)([transform(func, part) for part in geom.geoms])
    else:
        raise ValueError('Type %r not recognized' % geom.type)


project = partial(
    transform,
    Proj(init='epsg:27700'),  # wgs84
    Proj(init='epsg:4326'))

test = GeoDataFrame.from_file('/home/cloudera/Documents/tesseract_1.4.7.tar/London.shp')

latLongGeo = transformToBNG(project, test['geometry'])
print "CHECK", latLongGeo

BLUE = '#6699cc'
poly = test['geometry'][0]
print type(poly)
fig = plt.figure()
ax = fig.gca()
ax.add_patch(PolygonPatch(poly, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2))
ax.axis('scaled')
plt.show()
